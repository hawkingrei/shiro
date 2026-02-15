"use client";

import { useDeferredValue, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { format } from "sql-formatter";
import ReactDiffViewer, { DiffMethod } from "react-diff-viewer-continued";
import {
  caseArchiveURL,
  caseID,
  caseReportURL,
  isHTTPURL,
  objectURL,
  similarCasesURL,
} from "../lib/report-utils";

type FileContent = {
  name?: string;
  content?: string;
  truncated?: boolean;
};

type CaseEntry = {
  id: string;
  dir: string;
  oracle: string;
  timestamp: string;
  tidb_version: string;
  tidb_commit: string;
  error_reason?: string;
  plan_signature: string;
  plan_signature_format: string;
  expected: string;
  actual: string;
  error: string;
  groundtruth_dsg_mismatch_reason?: string;
  flaky?: boolean;
  norec_optimized_sql: string;
  norec_unoptimized_sql: string;
  norec_predicate: string;
  case_id?: string;
  case_dir: string;
  archive_name?: string;
  archive_codec?: string;
  archive_url?: string;
  report_url?: string;
  sql: string[];
  plan_replayer: string;
  upload_location: string;
  details: Record<string, unknown> | null;
  files: Record<string, FileContent>;
  summary_url?: string;
  search_blob?: string;
  detail_loaded?: boolean;
  labels?: string[];
  linked_issue?: string;
  replay_sql?: string;
  minimize_status?: string;
};

type CaseMetaState = {
  labels: string[];
  linkedIssue: string;
  draftLabels: string;
  draftIssue: string;
  newLabel: string;
  loading: boolean;
  saving: boolean;
  loaded: boolean;
  error: string;
};

type ReportPayload = {
  generated_at: string;
  source: string;
  index_version?: number;
  case_count?: number;
  cases: CaseEntry[];
};

type SimilarCase = {
  case_id?: string;
  oracle?: string;
  error_reason?: string;
  error_type?: string;
  labels?: string[];
  similarity_score?: number;
  report_url?: string;
  archive_url?: string;
};

type SimilarPayload = {
  target?: SimilarCase;
  answer?: string;
  matches?: SimilarCase[];
};

type IndexedCase = {
  entry: CaseEntry;
  key: string;
  caseIDValue: string;
  reasonLabel: string;
  searchBlob: string;
};

const detailString = (details: Record<string, unknown> | null, key: string): string => {
  if (!details) return "";
  const value = details[key];
  return typeof value === "string" ? value : "";
};

const detailBool = (details: Record<string, unknown> | null, key: string): boolean => {
  if (!details) return false;
  const value = details[key];
  if (typeof value === "boolean") return value;
  if (typeof value === "string") return value.toLowerCase() === "true";
  return false;
};

const normalizeLabels = (value: unknown): string[] => {
  if (!Array.isArray(value)) {
    return [];
  }
  const dedup = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") continue;
    const label = item.trim();
    if (label) {
      dedup.add(label);
    }
  }
  return Array.from(dedup.values());
};

const parseLabelInput = (value: string): string[] => {
  const dedup = new Set<string>();
  for (const part of value.split(",")) {
    const label = part.trim();
    if (label) {
      dedup.add(label);
    }
  }
  return Array.from(dedup.values());
};

const workerAuthHeaders = (token: string): Record<string, string> => {
  const trimmed = token.trim();
  if (!trimmed) {
    return {};
  }
  return { Authorization: `Bearer ${trimmed}` };
};

type BootstrapResult = {
  ok: boolean;
  complete: boolean;
  collected: Map<string, { labels: string[]; linkedIssue: string }>;
};

const emptyCaseMeta = (): CaseMetaState => ({
  labels: [],
  linkedIssue: "",
  draftLabels: "",
  draftIssue: "",
  newLabel: "",
  loading: false,
  saving: false,
  loaded: false,
  error: "",
});

const caseHasTruncation = (c: CaseEntry): boolean => {
  return detailBool(c.details, "expected_rows_truncated") || detailBool(c.details, "actual_rows_truncated");
};

const workerBaseURLEnv = (process.env.NEXT_PUBLIC_WORKER_BASE_URL || "").trim().replace(/\/+$/, "");
const reportsBaseURL = (process.env.NEXT_PUBLIC_REPORTS_BASE_URL || "").trim().replace(/\/+$/, "");
const issueBaseURLEnv = (process.env.NEXT_PUBLIC_ISSUE_BASE_URL || "").trim().replace(/\/+$/, "");
const workerTokenStorageKey = "shiro_worker_write_token";
const caseMetaCacheStorageKey = "shiro_case_meta_cache_v1";
const searchDebounceMS = 180;
const casesPerPage = 30;
const reservedFileKeys = new Set([
  "case.sql",
  "inserts.sql",
  "plan_replayer.zip",
  "data.tsv",
  "schema.sql",
  "report.json",
]);

const copyText = async (label: string, text: string) => {
  if (!text) return;
  try {
    await navigator.clipboard.writeText(text);
    console.log(`copied ${label}`);
  } catch {
    const fallback = document.createElement("textarea");
    fallback.value = text;
    fallback.style.position = "fixed";
    fallback.style.opacity = "0";
    document.body.appendChild(fallback);
    fallback.focus();
    fallback.select();
    try {
      document.execCommand("copy");
      console.log(`copied ${label}`);
    } finally {
      document.body.removeChild(fallback);
    }
  }
};

const issueLinkFrom = (value: string): { href: string; label: string } | null => {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (isHTTPURL(trimmed)) {
    return { href: trimmed, label: trimmed };
  }
  const repoMatch = trimmed.match(/^([\w.-]+\/[\w.-]+)#(\d+)$/);
  if (repoMatch) {
    const repo = repoMatch[1];
    const num = repoMatch[2];
    return { href: `https://github.com/${repo}/issues/${num}`, label: `${repo}#${num}` };
  }
  const numMatch = trimmed.match(/^#?(\d+)$/);
  if (numMatch && issueBaseURLEnv) {
    const num = numMatch[1];
    return { href: `${issueBaseURLEnv}/${num}`, label: `#${num}` };
  }
  return null;
};

const presetLabels = ["not bug", "critical", "major", "moderate", "minor", "Enhancement"];

const togglePresetLabel = (current: string, label: string): string => {
  const labels = parseLabelInput(current);
  const idx = labels.findIndex((item) => item.toLowerCase() === label.toLowerCase());
  if (idx >= 0) {
    labels.splice(idx, 1);
  } else {
    labels.push(label);
  }
  return labels.join(", ");
};

const LabelRow = ({ label, onCopy }: { label: string; onCopy?: () => void }) => {
  return (
    <div className="label-row">
      <div className="label">{label}</div>
      {onCopy && (
        <button className="copy-btn" type="button" onClick={onCopy}>
          Copy
        </button>
      )}
    </div>
  );
};

const formatSQL = (sql: string) => {
  if (!sql.trim()) return sql;
  try {
    return format(sql, { language: "mysql" });
  } catch {
    return sql;
  }
};

type PersistedCaseMeta = {
  labels?: string[];
  linkedIssue?: string;
};

const loadPersistedCaseMeta = (): Record<string, PersistedCaseMeta> => {
  try {
    const raw = window.localStorage.getItem(caseMetaCacheStorageKey) || "";
    if (!raw.trim()) {
      return {};
    }
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    const out: Record<string, PersistedCaseMeta> = {};
    for (const [caseIDValue, value] of Object.entries(parsed)) {
      if (!caseIDValue.trim()) {
        continue;
      }
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        continue;
      }
      const row = value as Record<string, unknown>;
      const labels = normalizeLabels(row.labels);
      const linkedIssue = typeof row.linkedIssue === "string" ? row.linkedIssue.trim() : "";
      out[caseIDValue] = { labels, linkedIssue };
    }
    return out;
  } catch {
    return {};
  }
};

const persistCaseMeta = (metaByID: Record<string, CaseMetaState>) => {
  try {
    const payload: Record<string, PersistedCaseMeta> = {};
    for (const [caseIDValue, meta] of Object.entries(metaByID)) {
      if (!caseIDValue.trim()) {
        continue;
      }
      const labels = normalizeLabels(meta.labels);
      const linkedIssue = (meta.linkedIssue || "").trim();
      if (labels.length === 0 && !linkedIssue) {
        continue;
      }
      payload[caseIDValue] = { labels, linkedIssue };
    }
    window.localStorage.setItem(caseMetaCacheStorageKey, JSON.stringify(payload));
  } catch {
    // Ignore localStorage failures in restricted/private contexts.
  }
};

const caseEmbeddedLabels = (entry: CaseEntry): string[] => {
  const labels = normalizeLabels(entry.labels);
  if (labels.length > 0) {
    return labels;
  }
  if (!entry.details) {
    return [];
  }
  return normalizeLabels(entry.details["labels"]);
};

const caseEmbeddedIssue = (entry: CaseEntry): string => {
  const linkedIssue = (entry.linked_issue || "").trim();
  if (linkedIssue) {
    return linkedIssue;
  }
  if (!entry.details) {
    return "";
  }
  const raw = entry.details["linked_issue"];
  return typeof raw === "string" ? raw.trim() : "";
};

const caseResolvedLabels = (entry: CaseEntry, meta: CaseMetaState | null): string[] => {
  if (meta?.loaded) {
    return normalizeLabels(meta.labels);
  }
  return caseEmbeddedLabels(entry);
};

const caseResolvedIssue = (entry: CaseEntry, meta: CaseMetaState | null): string => {
  if (meta?.loaded) {
    return (meta.linkedIssue || "").trim();
  }
  return caseEmbeddedIssue(entry);
};

const formatExplain = (text: string) => {
  if (!text.trim()) return text;
  const lines = text.replace(/\r/g, "").split("\n");
  const rows = lines.map((line) => {
    if (!line.includes("\t")) return [line];
    const match = line.match(/^(\s*)(.*)$/);
    const prefix = match ? match[1] : "";
    const rest = match ? match[2] : line;
    const parts = rest.split(/\t+/).map((part) => part.trim());
    if (parts.length > 0) {
      parts[0] = prefix + parts[0];
    }
    return parts;
  });
  const widths: number[] = [];
  rows.forEach((cols) => {
    if (cols.length <= 1) return;
    cols.forEach((col, idx) => {
      const len = col.length;
      widths[idx] = Math.max(widths[idx] || 0, len);
    });
  });
  return rows
    .map((cols) => {
      if (cols.length <= 1) {
        return cols[0] || "";
      }
      return cols.map((col, idx) => col.padEnd(widths[idx] || 0)).join("  ").trimEnd();
    })
    .join("\n");
};

type CaseBlock = { label: string; content: ReactNode; copyText?: string };

const renderBlock = (block: CaseBlock | null) => {
  if (!block) {
    return <div className="case__block case__block--empty" />;
  }
  return (
    <div className="case__block">
      <LabelRow label={block.label} onCopy={block.copyText ? () => copyText(block.label, block.copyText || "") : undefined} />
      {block.content}
    </div>
  );
};

const reasonForCase = (c: CaseEntry): string => {
  const explicit = (c.error_reason || "").trim();
  if (explicit) return explicit;
  if (c.error) return "exec_error";
  const details = c.details as Record<string, unknown> | null;
  const missWithoutWarnings = Boolean(details && details.miss_without_warnings);
  const expected = c.expected || "";
  const actual = c.actual || "";
  if (missWithoutWarnings) return "cache_miss_no_warnings";
  if (expected.startsWith("cnt=") && actual.startsWith("cnt=") && expected !== actual) return "result_mismatch";
  if (expected.startsWith("last_plan_from_cache=") && expected !== actual) return "cache_miss";
  return "other";
};

const caseRenderKey = (c: CaseEntry, index: number): string => {
  return caseID(c) || `case-${index}`;
};

const buildCaseSearchBlob = (c: CaseEntry): string => {
  return [
    c.oracle,
    c.error_reason,
    c.error,
    c.expected,
    c.actual,
    c.groundtruth_dsg_mismatch_reason,
    c.norec_optimized_sql,
    c.norec_unoptimized_sql,
    c.norec_predicate,
    c.tidb_version,
    c.tidb_commit,
    c.plan_signature,
    c.plan_signature_format,
    c.case_id,
    c.case_dir,
    c.upload_location,
    c.replay_sql,
    c.minimize_status,
    ...(c.labels || []),
    c.linked_issue,
    ...(c.sql || []),
    c.details && Object.keys(c.details).length > 0 ? JSON.stringify(c.details) : null,
  ]
    .filter((value) => Boolean(value))
    .join(" ")
    .toLowerCase();
};

const asString = (value: unknown): string => {
  return typeof value === "string" ? value : "";
};

const asBoolean = (value: unknown): boolean => {
  return typeof value === "boolean" ? value : false;
};

const asStringArray = (value: unknown): string[] => {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.filter((item): item is string => typeof item === "string");
};

const asStringRecord = (value: unknown): Record<string, unknown> | null => {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
};

const normalizeFiles = (value: unknown): Record<string, FileContent> => {
  const record = asStringRecord(value);
  if (!record) {
    return {};
  }
  const normalized: Record<string, FileContent> = {};
  for (const [key, item] of Object.entries(record)) {
    const file = asStringRecord(item);
    if (!file) {
      continue;
    }
    const truncatedRaw = file.truncated;
    const truncated =
      typeof truncatedRaw === "boolean"
        ? truncatedRaw
        : typeof truncatedRaw === "string"
        ? truncatedRaw.toLowerCase() === "true"
        : false;
    normalized[key] = {
      name: asString(file.name),
      content: asString(file.content),
      truncated,
    };
  }
  return normalized;
};

const normalizeCaseEntry = (value: unknown): CaseEntry | null => {
  const record = asStringRecord(value);
  if (!record) {
    return null;
  }

  const sql = asStringArray(record.sql);
  const details = asStringRecord(record.details);
  const files = normalizeFiles(record.files);
  const inferredDetailLoaded =
    sql.length > 0 ||
    (details !== null && Object.keys(details).length > 0) ||
    Object.keys(files).length > 0 ||
    asString(record.norec_optimized_sql).trim().length > 0 ||
    asString(record.norec_unoptimized_sql).trim().length > 0;

  const normalized: CaseEntry = {
    id: asString(record.id),
    dir: asString(record.dir),
    oracle: asString(record.oracle),
    timestamp: asString(record.timestamp),
    tidb_version: asString(record.tidb_version),
    tidb_commit: asString(record.tidb_commit),
    error_reason: asString(record.error_reason),
    plan_signature: asString(record.plan_signature),
    plan_signature_format: asString(record.plan_signature_format),
    expected: asString(record.expected),
    actual: asString(record.actual),
    error: asString(record.error),
    groundtruth_dsg_mismatch_reason: asString(record.groundtruth_dsg_mismatch_reason),
    flaky: asBoolean(record.flaky),
    norec_optimized_sql: asString(record.norec_optimized_sql),
    norec_unoptimized_sql: asString(record.norec_unoptimized_sql),
    norec_predicate: asString(record.norec_predicate),
    case_id: asString(record.case_id),
    case_dir: asString(record.case_dir),
    archive_name: asString(record.archive_name),
    archive_codec: asString(record.archive_codec),
    archive_url: asString(record.archive_url),
    report_url: asString(record.report_url),
    sql,
    plan_replayer: asString(record.plan_replayer),
    upload_location: asString(record.upload_location),
    details,
    files,
    summary_url: asString(record.summary_url),
    search_blob: asString(record.search_blob),
    detail_loaded: typeof record.detail_loaded === "boolean" ? record.detail_loaded : inferredDetailLoaded,
    labels: normalizeLabels(record.labels),
    linked_issue: asString(record.linked_issue),
    replay_sql: asString(record.replay_sql),
    minimize_status: asString(record.minimize_status),
  };

  if (!normalized.summary_url) {
    normalized.summary_url = caseReportURL(normalized);
  }
  if (!normalized.search_blob) {
    normalized.search_blob = buildCaseSearchBlob(normalized);
  }
  return normalized;
};

const normalizeReportPayload = (value: unknown): ReportPayload | null => {
  const record = asStringRecord(value);
  if (!record) {
    return null;
  }
  const rawCases = Array.isArray(record.cases) ? record.cases : [];
  const cases: CaseEntry[] = [];
  for (const raw of rawCases) {
    const normalized = normalizeCaseEntry(raw);
    if (normalized) {
      cases.push(normalized);
    }
  }
  return {
    generated_at: asString(record.generated_at),
    source: asString(record.source),
    index_version: typeof record.index_version === "number" ? record.index_version : undefined,
    case_count: typeof record.case_count === "number" ? record.case_count : undefined,
    cases,
  };
};

const resolveSummaryURL = (summaryURL: string, manifestBaseURL: string): string => {
  const trimmed = summaryURL.trim();
  if (!trimmed) {
    return "";
  }
  if (isHTTPURL(trimmed)) {
    return trimmed;
  }
  const rel = trimmed.replace(/^\.\//, "");
  const base = manifestBaseURL.trim();
  if (isHTTPURL(base)) {
    return objectURL(base, rel);
  }
  if (!base || base === ".") {
    if (trimmed.startsWith("./") || trimmed.startsWith("/")) {
      return trimmed;
    }
    return `./${rel}`;
  }
  return `${base.replace(/\/+$/, "")}/${rel}`;
};

export default function Page() {
  const [payload, setPayload] = useState<ReportPayload | null>(null);
  const [manifestBaseURL, setManifestBaseURL] = useState(reportsBaseURL || ".");
  const [caseDetailLoadingByKey, setCaseDetailLoadingByKey] = useState<Record<string, boolean>>({});
  const [caseDetailErrorByKey, setCaseDetailErrorByKey] = useState<Record<string, string>>({});
  const [similarByCase, setSimilarByCase] = useState<Record<string, SimilarPayload>>({});
  const [similarLoadingByCase, setSimilarLoadingByCase] = useState<Record<string, boolean>>({});
  const [similarErrorByCase, setSimilarErrorByCase] = useState<Record<string, string>>({});
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [oracle, setOracle] = useState("");
  const [commit, setCommit] = useState("");
  const [planSig, setPlanSig] = useState("");
  const [planSigFormat, setPlanSigFormat] = useState("");
  const [onlyErrors, setOnlyErrors] = useState(false);
  const [showExplainSame, setShowExplainSame] = useState(false);
  const [reason, setReason] = useState("");
  const [labelFilter, setLabelFilter] = useState("");
  const [page, setPage] = useState(1);
  const [error, setError] = useState<string | null>(null);
  const [workerBaseURL, setWorkerBaseURL] = useState(workerBaseURLEnv);
  const [workerToken, setWorkerToken] = useState("");
  const [caseMetaByID, setCaseMetaByID] = useState<Record<string, CaseMetaState>>({});
  const [activeMetaID, setActiveMetaID] = useState<string | null>(null);
  const [expandedCaseKeys, setExpandedCaseKeys] = useState<Record<string, boolean>>({});
  const metaBootstrapInFlight = useRef(false);
  const metaBootstrapRunID = useRef(0);
  const caseMetaByIDRef = useRef(caseMetaByID);
  const activeMetaIDRef = useRef(activeMetaID);
  const pageMountedRef = useRef(true);
  const caseDetailAbortByKeyRef = useRef<Record<string, AbortController>>({});
  const caseDetailRunIDByKeyRef = useRef<Record<string, number>>({});
  const caseDetailNextRunIDRef = useRef(1);

  useEffect(() => {
    const stored = window.sessionStorage.getItem(workerTokenStorageKey) || "";
    if (stored) {
      setWorkerToken(stored);
    }
  }, []);

  useEffect(() => {
    const persisted = loadPersistedCaseMeta();
    const entries = Object.entries(persisted);
    if (entries.length === 0) {
      return;
    }
    setCaseMetaByID((prev) => {
      const next = { ...prev };
      for (const [caseIDValue, meta] of entries) {
        const current = next[caseIDValue] || emptyCaseMeta();
        const labels = normalizeLabels(meta.labels);
        const linkedIssue = typeof meta.linkedIssue === "string" ? meta.linkedIssue.trim() : "";
        next[caseIDValue] = {
          ...current,
          labels,
          linkedIssue,
          draftLabels: current.draftLabels || labels.join(", "),
          draftIssue: current.draftIssue || linkedIssue,
          loaded: true,
          error: "",
        };
      }
      return next;
    });
  }, []);

  useEffect(() => {
    persistCaseMeta(caseMetaByID);
  }, [caseMetaByID]);

  useEffect(() => {
    if (workerBaseURLEnv) {
      return;
    }
    const origin = window.location.origin.replace(/\/+$/, "");
    setWorkerBaseURL(origin);
  }, []);

  useEffect(() => {
    if (!activeMetaID) {
      return;
    }
    const handler = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setActiveMetaID(null);
      }
    };
    window.addEventListener("keydown", handler);
    return () => {
      window.removeEventListener("keydown", handler);
    };
  }, [activeMetaID]);

  useEffect(() => {
    pageMountedRef.current = true;
    return () => {
      pageMountedRef.current = false;
      const inFlight = caseDetailAbortByKeyRef.current;
      for (const controller of Object.values(inFlight)) {
        controller.abort();
      }
      caseDetailAbortByKeyRef.current = {};
      caseDetailRunIDByKeyRef.current = {};
    };
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setDebouncedQuery(query);
    }, searchDebounceMS);
    return () => {
      window.clearTimeout(timer);
    };
  }, [query]);

  useEffect(() => {
    caseMetaByIDRef.current = caseMetaByID;
  }, [caseMetaByID]);

  useEffect(() => {
    activeMetaIDRef.current = activeMetaID;
  }, [activeMetaID]);

  useEffect(() => {
    let canceled = false;
    const load = async () => {
      let lastErr: Error | null = null;
      const candidates: Array<{ url: string; base: string }> = [];
      if (reportsBaseURL) {
        candidates.push(
          { url: `${reportsBaseURL}/reports.index.json`, base: reportsBaseURL },
          { url: `${reportsBaseURL}/reports.json`, base: reportsBaseURL },
          { url: `${reportsBaseURL}/report.json`, base: reportsBaseURL },
        );
      }
      candidates.push(
        { url: "./reports.index.json", base: "." },
        { url: "./reports.json", base: "." },
        { url: "./report.json", base: "." },
      );
      for (const candidate of candidates) {
        try {
          const res = await fetch(candidate.url, { cache: "no-cache" });
          if (!res.ok) {
            throw new Error(`failed to load ${candidate.url}: ${res.status}`);
          }
          const raw = await res.json();
          const data = normalizeReportPayload(raw);
          if (!data) {
            throw new Error(`invalid report payload from ${candidate.url}`);
          }
          if (!canceled) {
            setPayload(data);
            setManifestBaseURL(candidate.base);
            setCaseDetailLoadingByKey({});
            setCaseDetailErrorByKey({});
            setError(null);
          }
          return;
        } catch (err) {
          lastErr = err instanceof Error ? err : new Error(String(err));
        }
      }
      if (!canceled && lastErr) {
        setError(lastErr.message);
      }
    };
    void load();
    return () => {
      canceled = true;
    };
  }, []);

  const patchCaseEntry = (caseKey: string, updater: (current: CaseEntry) => CaseEntry) => {
    if (!caseKey) {
      return;
    }
    setPayload((prev) => {
      if (!prev) {
        return prev;
      }
      let changed = false;
      const nextCases = prev.cases.map((entry, index) => {
        if (caseRenderKey(entry, index) !== caseKey) {
          return entry;
        }
        changed = true;
        return updater(entry);
      });
      if (!changed) {
        return prev;
      }
      return { ...prev, cases: nextCases };
    });
  };

  const cancelCaseDetail = (caseKey: string) => {
    const controller = caseDetailAbortByKeyRef.current[caseKey];
    if (!controller) {
      return;
    }
    controller.abort();
    delete caseDetailAbortByKeyRef.current[caseKey];
    delete caseDetailRunIDByKeyRef.current[caseKey];
    if (pageMountedRef.current) {
      setCaseDetailLoadingByKey((prev) => ({ ...prev, [caseKey]: false }));
    }
  };

  const ensureCaseDetail = async (caseKey: string, entry: CaseEntry) => {
    if (!caseKey || entry.detail_loaded) {
      return;
    }
    if (caseDetailAbortByKeyRef.current[caseKey]) {
      return;
    }
    const summaryURL = resolveSummaryURL(entry.summary_url || caseReportURL(entry), manifestBaseURL);
    if (!summaryURL) {
      if (pageMountedRef.current) {
        setCaseDetailErrorByKey((prev) => ({ ...prev, [caseKey]: "detail URL missing" }));
      }
      return;
    }
    const controller = new AbortController();
    const runID = caseDetailNextRunIDRef.current;
    caseDetailNextRunIDRef.current += 1;
    caseDetailAbortByKeyRef.current[caseKey] = controller;
    caseDetailRunIDByKeyRef.current[caseKey] = runID;
    if (pageMountedRef.current) {
      setCaseDetailLoadingByKey((prev) => ({ ...prev, [caseKey]: true }));
      setCaseDetailErrorByKey((prev) => ({ ...prev, [caseKey]: "" }));
    }
    const isCurrentRun = () => {
      return pageMountedRef.current && caseDetailRunIDByKeyRef.current[caseKey] === runID;
    };
    try {
      const resp = await fetch(summaryURL, { cache: "no-cache", signal: controller.signal });
      if (!resp.ok) {
        if (isCurrentRun()) {
          setCaseDetailErrorByKey((prev) => ({ ...prev, [caseKey]: `load failed (${resp.status})` }));
        }
        return;
      }
      const raw = await resp.json();
      const detail = normalizeCaseEntry(raw);
      if (!detail) {
        if (isCurrentRun()) {
          setCaseDetailErrorByKey((prev) => ({ ...prev, [caseKey]: "invalid case summary payload" }));
        }
        return;
      }
      if (!isCurrentRun()) {
        return;
      }
      patchCaseEntry(caseKey, (current) => ({
        ...current,
        ...detail,
        summary_url: current.summary_url || detail.summary_url,
        search_blob: detail.search_blob || current.search_blob || buildCaseSearchBlob(detail),
        detail_loaded: true,
      }));
      setCaseDetailErrorByKey((prev) => ({ ...prev, [caseKey]: "" }));
    } catch (err) {
      if (err instanceof Error && err.name === "AbortError") {
        return;
      }
      if (isCurrentRun()) {
        setCaseDetailErrorByKey((prev) => ({ ...prev, [caseKey]: "load failed" }));
      }
    } finally {
      if (caseDetailRunIDByKeyRef.current[caseKey] !== runID) {
        return;
      }
      delete caseDetailRunIDByKeyRef.current[caseKey];
      delete caseDetailAbortByKeyRef.current[caseKey];
      if (pageMountedRef.current) {
        setCaseDetailLoadingByKey((prev) => ({ ...prev, [caseKey]: false }));
      }
    }
  };

  const updateCaseMetaState = (caseID: string, updater: (current: CaseMetaState) => CaseMetaState) => {
    setCaseMetaByID((prev) => {
      const current = prev[caseID] || emptyCaseMeta();
      const next = updater(current);
      return { ...prev, [caseID]: next };
    });
  };

  const openMetaEditor = (caseID: string) => {
    if (!caseID) return;
    void ensureCaseMeta(caseID);
    setActiveMetaID(caseID);
  };

  const ensureCaseMeta = async (caseID: string) => {
    if (!workerBaseURL || !caseID) {
      return;
    }
    const current = caseMetaByID[caseID];
    if (current?.loaded || current?.loading) {
      return;
    }
    updateCaseMetaState(caseID, (state) => ({ ...state, loading: true, error: "" }));
    try {
      const resp = await fetch(`${workerBaseURL}/api/v1/cases/${encodeURIComponent(caseID)}`, {
        cache: "no-cache",
        headers: workerAuthHeaders(workerToken),
      });
      if (resp.status === 404) {
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          loading: false,
          loaded: true,
          error: "",
        }));
        return;
      }
      if (resp.status === 401) {
        const silentUnauthorized = !workerToken.trim();
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          loading: false,
          loaded: state.loaded,
          error: silentUnauthorized ? "" : "unauthorized",
        }));
        return;
      }
      if (!resp.ok) {
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          loading: false,
          loaded: state.loaded,
          error: `load failed (${resp.status})`,
        }));
        return;
      }
      const payload = (await resp.json()) as Record<string, unknown>;
      const labels = normalizeLabels(payload.labels);
      const linkedIssue = typeof payload.linked_issue === "string" ? payload.linked_issue.trim() : "";
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        labels,
        linkedIssue,
        draftLabels: labels.join(", "),
        draftIssue: linkedIssue,
        loading: false,
        loaded: true,
        error: "",
      }));
    } catch {
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        loading: false,
        loaded: state.loaded,
        error: "load failed",
      }));
    }
  };

  const cases = useMemo(() => payload?.cases ?? [], [payload]);

  useEffect(() => {
    if (!workerBaseURL || cases.length === 0 || metaBootstrapInFlight.current) {
      return;
    }
    const caseIDs = Array.from(
      new Set(
        cases
          .map((entry) => (caseID(entry) || "").trim())
          .filter((id) => id.length > 0),
      ),
    );
    if (caseIDs.length === 0) {
      return;
    }
    const needBootstrap = caseIDs.some((cid) => {
      const meta = caseMetaByIDRef.current[cid];
      return !meta || (!meta.loaded && !meta.loading);
    });
    if (!needBootstrap) {
      return;
    }

    let canceled = false;
    const runID = metaBootstrapRunID.current + 1;
    metaBootstrapRunID.current = runID;
    metaBootstrapInFlight.current = true;

    const loadAllMetadata = async (): Promise<BootstrapResult> => {
      const caseIDSet = new Set(caseIDs);
      const collected = new Map<string, { labels: string[]; linkedIssue: string }>();
      const limit = 500;
      let offset = 0;
      let complete = false;

      for (;;) {
        const resp = await fetch(`${workerBaseURL}/api/v1/cases?limit=${limit}&offset=${offset}`, {
          cache: "no-cache",
          headers: workerAuthHeaders(workerToken),
        });
        if (resp.status === 401 || !resp.ok) {
          return { ok: false, complete: false, collected };
        }
        const payload = (await resp.json()) as Record<string, unknown>;
        const rows = Array.isArray(payload.cases) ? payload.cases : [];
        for (const raw of rows) {
          if (!raw || typeof raw !== "object") {
            continue;
          }
          const row = raw as Record<string, unknown>;
          const cid = typeof row.case_id === "string" ? row.case_id.trim() : "";
          if (!cid || !caseIDSet.has(cid)) {
            continue;
          }
          collected.set(cid, {
            labels: normalizeLabels(row.labels),
            linkedIssue: typeof row.linked_issue === "string" ? row.linked_issue.trim() : "",
          });
        }
        const total = typeof payload.total === "number" ? payload.total : 0;
        offset += rows.length;
        if (rows.length === 0 || (total > 0 && offset >= total) || collected.size >= caseIDs.length || (total === 0 && rows.length < limit)) {
          complete = true;
          break;
        }
      }

      return { ok: true, complete, collected };
    };

    void loadAllMetadata()
      .then((result) => {
        if (canceled || !result.ok) {
          return;
        }
        setCaseMetaByID((prev) => {
          const next = { ...prev };
          for (const cid of caseIDs) {
            const current = next[cid] || emptyCaseMeta();
            const loaded = result.collected.get(cid);
            if (loaded) {
              const loadedDraftLabels = loaded.labels.join(", ");
              const loadedDraftIssue = loaded.linkedIssue;
              const currentCanonicalLabels = current.labels.join(", ");
              const currentCanonicalIssue = current.linkedIssue;
              const currentDraftLabels = current.draftLabels || "";
              const currentDraftIssue = current.draftIssue || "";
              const shouldProtectDraft = current.loading || current.saving || activeMetaIDRef.current === cid;
              const draftLabelsChanged =
                currentDraftLabels.trim() !== "" &&
                currentDraftLabels.trim() !== currentCanonicalLabels.trim();
              const draftIssueChanged =
                currentDraftIssue.trim() !== "" &&
                currentDraftIssue.trim() !== currentCanonicalIssue.trim();
              const keepDraftLabels = shouldProtectDraft || draftLabelsChanged;
              const keepDraftIssue = shouldProtectDraft || draftIssueChanged;

              next[cid] = {
                ...current,
                labels: loaded.labels,
                linkedIssue: loaded.linkedIssue,
                draftLabels: keepDraftLabels ? currentDraftLabels : loadedDraftLabels,
                draftIssue: keepDraftIssue ? currentDraftIssue : loadedDraftIssue,
                loaded: true,
                loading: false,
                error: "",
              };
              continue;
            }
            if (result.complete && !current.loaded && !current.loading && !current.saving) {
              next[cid] = {
                ...current,
                loaded: true,
                loading: false,
                error: "",
              };
            }
          }
          return next;
        });
      })
      .catch(() => {
        // Ignore bootstrap failures and keep per-case lazy loading available.
      })
      .finally(() => {
        if (metaBootstrapRunID.current === runID) {
          metaBootstrapInFlight.current = false;
        }
      });

    return () => {
      canceled = true;
      metaBootstrapInFlight.current = false;
    };
  }, [workerBaseURL, workerToken, cases]);

  const saveCaseMeta = async (caseID: string) => {
    if (!workerBaseURL || !caseID) {
      return;
    }
    if (!workerToken.trim()) {
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        error: "write token required",
      }));
      return;
    }
    const current = caseMetaByID[caseID] || emptyCaseMeta();
    if (current.loading) {
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        error: "metadata loading",
      }));
      return;
    }
    const labels = parseLabelInput(current.draftLabels);
    const linkedIssue = current.draftIssue.trim();
    const payload = {
      labels,
      linked_issue: linkedIssue,
    };
    updateCaseMetaState(caseID, (state) => ({ ...state, saving: true, error: "" }));
    try {
      const resp = await fetch(`${workerBaseURL}/api/v1/cases/${encodeURIComponent(caseID)}`, {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
          ...workerAuthHeaders(workerToken),
        },
        body: JSON.stringify(payload),
      });
      if (!resp.ok) {
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          saving: false,
          error: `save failed (${resp.status})`,
        }));
        return;
      }
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        labels,
        linkedIssue,
        draftLabels: labels.join(", "),
        draftIssue: linkedIssue,
        saving: false,
        loaded: true,
        error: "",
      }));
    } catch {
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        saving: false,
        error: "save failed",
      }));
    }
  };
  const deferredQuery = useDeferredValue(debouncedQuery);
  const q = deferredQuery.trim().toLowerCase();
  const indexedCases = useMemo<IndexedCase[]>(() => {
    return cases.map((entry, index) => ({
      entry,
      key: caseRenderKey(entry, index),
      caseIDValue: (caseID(entry) || "").trim(),
      reasonLabel: reasonForCase(entry),
      searchBlob: (entry.search_blob || buildCaseSearchBlob(entry)).toLowerCase(),
    }));
  }, [cases]);

  const oracleOptions = useMemo(() => {
    return Array.from(new Set(cases.map((c) => c.oracle).filter(Boolean))).sort();
  }, [cases]);

  const commitOptions = useMemo(() => {
    return Array.from(new Set(cases.map((c) => c.tidb_commit).filter(Boolean))).sort();
  }, [cases]);

  const planSigOptions = useMemo(() => {
    return Array.from(new Set(cases.map((c) => c.plan_signature).filter(Boolean))).sort();
  }, [cases]);

  const planSigFormatOptions = useMemo(() => {
    return Array.from(new Set(cases.map((c) => c.plan_signature_format).filter(Boolean))).sort();
  }, [cases]);

  const reasonOptions = useMemo(() => {
    return Array.from(new Set(indexedCases.map((item) => item.reasonLabel))).sort();
  }, [indexedCases]);

  const labelOptions = useMemo(() => {
    const labels = new Set<string>();
    indexedCases.forEach((item) => {
      const cid = item.caseIDValue;
      const meta = cid ? (caseMetaByID[cid] || null) : null;
      caseResolvedLabels(item.entry, meta).forEach((label) => {
        if (label) labels.add(label);
      });
    });
    return Array.from(labels.values()).sort();
  }, [indexedCases, caseMetaByID]);

  const filtered = useMemo(() => {
    return indexedCases.filter((item) => {
      const c = item.entry;
      if (oracle && c.oracle !== oracle) return false;
      if (commit && c.tidb_commit !== commit) return false;
      if (planSig) {
        const cand = (c.plan_signature || "").trim().toLowerCase();
        const target = planSig.trim().toLowerCase();
        if (cand !== target) return false;
      }
      if (planSigFormat && c.plan_signature_format !== planSigFormat) return false;
      if (onlyErrors && !c.error) return false;
      if (reason && item.reasonLabel !== reason) return false;
      if (labelFilter) {
        const cid = item.caseIDValue;
        const meta = cid ? (caseMetaByID[cid] || null) : null;
        const labels = caseResolvedLabels(c, meta);
        const match = labels.some((label) => label.toLowerCase() === labelFilter.toLowerCase());
        if (!match) return false;
      }
      if (!q) return true;
      return item.searchBlob.includes(q);
    });
  }, [indexedCases, oracle, commit, planSig, planSigFormat, onlyErrors, reason, labelFilter, caseMetaByID, q]);

  useEffect(() => {
    setPage(1);
  }, [oracle, commit, planSig, planSigFormat, onlyErrors, reason, labelFilter, q]);

  const totalPages = useMemo(() => {
    return Math.max(1, Math.ceil(filtered.length / casesPerPage));
  }, [filtered.length]);

  useEffect(() => {
    if (page > totalPages) {
      setPage(totalPages);
    }
  }, [page, totalPages]);

  const pagedCases = useMemo(() => {
    const start = (page - 1) * casesPerPage;
    return filtered.slice(start, start + casesPerPage);
  }, [filtered, page]);

  const loadSimilarCases = async (cid: string) => {
    const caseIDValue = (cid || "").trim();
    if (!workerBaseURL || !caseIDValue) return;
    setSimilarLoadingByCase((prev) => ({ ...prev, [caseIDValue]: true }));
    setSimilarErrorByCase((prev) => ({ ...prev, [caseIDValue]: "" }));
    try {
      const url = `${workerBaseURL}/api/v1/cases/${encodeURIComponent(caseIDValue)}/similar?limit=20&ai=1`;
      const res = await fetch(url, {
        cache: "no-cache",
        headers: workerAuthHeaders(workerToken),
      });
      if (!res.ok) {
        throw new Error(`failed to load similar cases: ${res.status}`);
      }
      const data: SimilarPayload = await res.json();
      setSimilarByCase((prev) => ({ ...prev, [caseIDValue]: data }));
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setSimilarErrorByCase((prev) => ({ ...prev, [caseIDValue]: msg }));
    } finally {
      setSimilarLoadingByCase((prev) => ({ ...prev, [caseIDValue]: false }));
    }
  };

  const summary = useMemo(() => {
    const byReason = new Map<string, number>();
    const byOracle = new Map<string, number>();
    let errorCount = 0;
    let flakyCount = 0;
    let truncCount = 0;
    filtered.forEach((item) => {
      const c = item.entry;
      const reasonLabel = item.reasonLabel;
      byReason.set(reasonLabel, (byReason.get(reasonLabel) || 0) + 1);
      if (c.oracle) {
        byOracle.set(c.oracle, (byOracle.get(c.oracle) || 0) + 1);
      }
      if (c.error) errorCount += 1;
      if (c.flaky) flakyCount += 1;
      if (caseHasTruncation(c)) truncCount += 1;
    });
    const sortCounts = (entries: Map<string, number>) => {
      return Array.from(entries.entries()).sort((a, b) => b[1] - a[1]);
    };
    return {
      total: filtered.length,
      errorCount,
      flakyCount,
      truncCount,
      reasons: sortCounts(byReason),
      oracles: sortCounts(byOracle),
    };
  }, [filtered]);
  const searchPending = query.trim() !== debouncedQuery.trim();
  const indexModeLabel = payload?.index_version ? `index v${payload.index_version}` : "full payload";

  if (error) {
    return <div className="page"><div className="error">Failed to load reports.index.json/reports.json/report.json: {error}</div></div>;
  }

  return (
    <div className="page">
      <header className="hero">
        <div className="hero__text">
          <div className="hero__kicker">Shiro Fuzzing</div>
          <h1>Case Report Index</h1>
          <p className="hero__sub">
            Static frontend reads <code>reports.index.json</code> first, then falls back to <code>reports.json</code> and <code>report.json</code>. Expand a case to load full detail on demand from <code>summary_url</code>. Set <code>NEXT_PUBLIC_REPORTS_BASE_URL</code> to load reports from a public bucket or CDN.
          </p>
          <div className="hero__meta">
            <span>Generated: {payload?.generated_at ?? "-"}</span>
            <span>Source: {payload?.source ?? "-"}</span>
            <span>Mode: {indexModeLabel}</span>
          </div>
        </div>
        <div className="hero__panel">
          <div className="panel__title">Filters</div>
          <div className="filters">
            <input
              type="search"
              placeholder="Search SQL, error, expected/actual..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
            <select value={oracle} onChange={(e) => setOracle(e.target.value)}>
              <option value="">All oracles</option>
              {oracleOptions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
            <select value={commit} onChange={(e) => setCommit(e.target.value)}>
              <option value="">All commits</option>
              {commitOptions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
            <select value={planSig} onChange={(e) => setPlanSig(e.target.value)}>
              <option value="">All plan signatures</option>
              {planSigOptions.map((item) => (
                <option key={item} value={item}>
                  {item.slice(0, 12)}
                </option>
              ))}
            </select>
            <select value={planSigFormat} onChange={(e) => setPlanSigFormat(e.target.value)}>
              <option value="">All plan formats</option>
              {planSigFormatOptions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
            <select value={reason} onChange={(e) => setReason(e.target.value)}>
              <option value="">All reasons</option>
              {reasonOptions.map((item) => (
                <option key={item} value={item}>
                  {item.replace(/_/g, " ")}
                </option>
              ))}
            </select>
            {labelOptions.length > 0 && (
              <select value={labelFilter} onChange={(e) => setLabelFilter(e.target.value)}>
                <option value="">All labels</option>
                {labelOptions.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </select>
            )}
            <label className="toggle">
              <input type="checkbox" checked={onlyErrors} onChange={(e) => setOnlyErrors(e.target.checked)} />
              Only errors
            </label>
            <label className="toggle">
              <input type="checkbox" checked={showExplainSame} onChange={(e) => setShowExplainSame(e.target.checked)} />
              Show EXPLAIN unchanged
            </label>
          </div>
          <div className="stats">
            Total: {cases.length} | Filtered: {filtered.length} | Page: {pagedCases.length}
            {searchPending ? " | Searching..." : ""}
          </div>
          {workerBaseURL && (
            <>
              <div className="panel__title">Settings</div>
              <div className="filters">
                <input
                  type="password"
                  placeholder="Worker write token (session only)"
                  value={workerToken}
                  onChange={(e) => {
                    const next = e.target.value;
                    setWorkerToken(next);
                    window.sessionStorage.setItem(workerTokenStorageKey, next);
                  }}
                />
              </div>
            </>
          )}
        </div>
      </header>

      <section className="summary">
        <div className="summary__card">
          <div className="summary__title">Summary</div>
          <div className="summary__stats">
            <div>
              <span className="summary__label">Filtered cases</span>
              <span className="summary__value">{summary.total}</span>
            </div>
            <div>
              <span className="summary__label">Errors</span>
              <span className="summary__value">{summary.errorCount}</span>
            </div>
            <div>
              <span className="summary__label">Flaky</span>
              <span className="summary__value">{summary.flakyCount}</span>
            </div>
            <div>
              <span className="summary__label">Rows truncated</span>
              <span className="summary__value">{summary.truncCount}</span>
            </div>
          </div>
        </div>
        <div className="summary__card">
          <div className="summary__title">Reasons</div>
          <div className="summary__list">
            {summary.reasons.map(([label, count]) => (
              <div key={label}>
                <span className="summary__label">{label.replace(/_/g, " ")}</span>
                <span className="summary__value">{count}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="summary__card">
          <div className="summary__title">Oracles</div>
          <div className="summary__list">
            {summary.oracles.map(([label, count]) => (
              <div key={label}>
                <span className="summary__label">{label}</span>
                <span className="summary__value">{count}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="pager">
        <button
          className="copy-btn"
          type="button"
          aria-label="Go to previous page"
          onClick={() => setPage((current) => Math.max(1, current - 1))}
          disabled={page <= 1}
        >
          Prev
        </button>
        <span>
          Page {page} / {totalPages}
        </span>
        <span>
          {filtered.length === 0
            ? "0 of 0"
            : `${(page - 1) * casesPerPage + 1}-${Math.min(page * casesPerPage, filtered.length)} of ${filtered.length}`}
        </span>
        <button
          className="copy-btn"
          type="button"
          aria-label="Go to next page"
          onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
          disabled={page >= totalPages}
        >
          Next
        </button>
      </section>

      <main className="cases">
        {pagedCases.map((item) => {
          const c = item.entry;
          const cid = item.caseIDValue;
          const caseKey = item.key;
          const isExpanded = Boolean(expandedCaseKeys[caseKey]);
          const detailLoading = isExpanded ? Boolean(caseDetailLoadingByKey[caseKey]) : false;
          const detailError = isExpanded ? (caseDetailErrorByKey[caseKey] || "").trim() : "";
          const detailLoaded = Boolean(c.detail_loaded);
          const meta = cid ? (caseMetaByID[cid] || emptyCaseMeta()) : null;
          const metaLabels = caseResolvedLabels(c, meta);
          const metaLabelPreview = metaLabels.slice(0, 3);
          const metaLabelExtra = metaLabels.length - metaLabelPreview.length;
          const metaIssue = caseResolvedIssue(c, meta);
          const metaIssueDisplay = metaIssue.length > 32 ? `${metaIssue.slice(0, 32)}...` : metaIssue;
          const archiveURL = isExpanded ? caseArchiveURL(c) : "";
          const downloadURL = archiveURL;
          const archiveName = isExpanded ? (c.archive_name || "").trim() : "";
          const similarURL = isExpanded ? similarCasesURL(workerBaseURL, c) : "";
          const similarPayload = isExpanded && cid ? similarByCase[cid] : undefined;
          const similarList = isExpanded ? similarPayload?.matches || [] : [];
          const similarAnswer = isExpanded ? (similarPayload?.answer || "").trim() : "";
          const similarLoading = isExpanded && cid ? Boolean(similarLoadingByCase[cid]) : false;
          const similarError = isExpanded && cid ? (similarErrorByCase[cid] || "").trim() : "";
          const reasonLabel = item.reasonLabel;
          const expectedSQL = isExpanded ? detailString(c.details, "replay_expected_sql") || c.norec_optimized_sql || "" : "";
          const actualSQL = isExpanded ? detailString(c.details, "replay_actual_sql") || c.norec_unoptimized_sql || "" : "";
          const replaySQL = isExpanded ? detailString(c.details, "replay_sql") || c.replay_sql || "" : "";
          const minimizeStatus = isExpanded
            ? detailString(c.details, "minimize_status") || c.minimize_status || ""
            : c.minimize_status || "";
          const norecPredicate = isExpanded ? c.norec_predicate || "" : "";
          const expectedRowsTruncated = detailBool(c.details, "expected_rows_truncated");
          const actualRowsTruncated = detailBool(c.details, "actual_rows_truncated");
          const expectedExplainRaw = isExpanded ? detailString(c.details, "expected_explain") : "";
          const actualExplainRaw = isExpanded ? detailString(c.details, "actual_explain") : "";
          const unoptimizedExplainRaw = isExpanded ? detailString(c.details, "unoptimized_explain") : "";
          const optimizedExplainRaw = isExpanded ? detailString(c.details, "optimized_explain") : "";
          const expectedExplain = isExpanded ? formatExplain(expectedExplainRaw) : "";
          const actualExplain = isExpanded ? formatExplain(actualExplainRaw) : "";
          const optimizedExplain = isExpanded ? formatExplain(optimizedExplainRaw) : "";
          const unoptimizedExplain = isExpanded ? formatExplain(unoptimizedExplainRaw) : "";
          const expectedActualDiff =
            expectedExplain && actualExplain ? { oldValue: expectedExplain, newValue: actualExplain } : null;
          const optimizedDiff =
            optimizedExplain && unoptimizedExplain ? { oldValue: unoptimizedExplain, newValue: optimizedExplain } : null;
          const expectedText = c.expected || "";
          const actualText = c.actual || "";
          const expectedBlock: CaseBlock | null = expectedText
            ? {
                label: "Expected",
                content: <pre>{expectedText}</pre>,
                copyText: expectedText,
              }
            : null;
          const actualBlock: CaseBlock | null = actualText
            ? {
                label: "Actual",
                content: <pre>{actualText}</pre>,
                copyText: actualText,
              }
            : null;
          const expectedSQLBlock: CaseBlock | null = expectedSQL
            ? {
                label: "Expected SQL",
                content: <pre>{formatSQL(expectedSQL)}</pre>,
                copyText: expectedSQL,
              }
            : null;
          const actualSQLBlock: CaseBlock | null = actualSQL
            ? {
                label: "Actual SQL",
                content: <pre>{formatSQL(actualSQL)}</pre>,
                copyText: actualSQL,
              }
            : null;
          const replaySQLBlock: CaseBlock | null = replaySQL
            ? {
                label: minimizeStatus ? `Min Repro SQL (${minimizeStatus})` : "Min Repro SQL",
                content: <pre>{formatSQL(replaySQL)}</pre>,
                copyText: replaySQL,
              }
            : null;
          const expectedExplainBlock: CaseBlock | null = expectedExplain
            ? {
                label: "Expected EXPLAIN",
                content: <pre>{expectedExplain}</pre>,
                copyText: expectedExplain,
              }
            : null;
          const actualExplainBlock: CaseBlock | null = actualExplain
            ? {
                label: "Actual EXPLAIN",
                content: <pre>{actualExplain}</pre>,
                copyText: actualExplain,
              }
            : null;
          const optimizedExplainBlock: CaseBlock | null = optimizedExplain
            ? {
                label: "Optimized EXPLAIN",
                content: <pre>{optimizedExplain}</pre>,
                copyText: optimizedExplain,
              }
            : null;
          const unoptimizedExplainBlock: CaseBlock | null = unoptimizedExplain
            ? {
                label: "Unoptimized EXPLAIN",
                content: <pre>{unoptimizedExplain}</pre>,
                copyText: unoptimizedExplain,
              }
            : null;
          const optimizedDiffBlock: CaseBlock | null = optimizedDiff
            ? {
                label: "EXPLAIN Diff (Unoptimized | Optimized)",
                content: (
                  <div className="diff-viewer">
                    <ReactDiffViewer
                      oldValue={optimizedDiff.oldValue}
                      newValue={optimizedDiff.newValue}
                      splitView
                      showDiffOnly={!showExplainSame}
                      useDarkTheme={false}
                      compareMethod={DiffMethod.WORDS_WITH_SPACE}
                      disableWordDiff={false}
                      extraLinesSurroundingDiff={2}
                    />
                  </div>
                ),
                copyText: optimizedDiff.newValue,
              }
            : null;
          const expectedActualDiffBlock: CaseBlock | null = expectedActualDiff
            ? {
                label: "EXPLAIN Diff (Expected | Actual)",
                content: (
                  <div className="diff-viewer">
                    <ReactDiffViewer
                      oldValue={expectedActualDiff.oldValue}
                      newValue={expectedActualDiff.newValue}
                      splitView
                      showDiffOnly={!showExplainSame}
                      useDarkTheme={false}
                      compareMethod={DiffMethod.WORDS_WITH_SPACE}
                      disableWordDiff={false}
                      extraLinesSurroundingDiff={2}
                    />
                  </div>
                ),
                copyText: expectedActualDiff.newValue,
              }
            : null;
          const pairedRows: Array<{ left: CaseBlock | null; right: CaseBlock | null }> = [
            { left: expectedBlock, right: actualBlock },
            { left: expectedSQLBlock, right: actualSQLBlock },
          ].filter((row) => row.left || row.right);
          const diffBlocks: CaseBlock[] = [];
          if (expectedActualDiffBlock) {
            diffBlocks.push(expectedActualDiffBlock);
          }
          if (optimizedDiffBlock) {
            diffBlocks.push(optimizedDiffBlock);
          }
          return (
            <details
              className="case"
              key={caseKey}
              onToggle={(event) => {
                const open = (event.currentTarget as HTMLDetailsElement).open;
                setExpandedCaseKeys((prev) => {
                  if (prev[caseKey] === open) {
                    return prev;
                  }
                  return { ...prev, [caseKey]: open };
                });
                if (open && cid) {
                  void ensureCaseDetail(caseKey, c);
                  void ensureCaseMeta(cid);
                } else if (open) {
                  void ensureCaseDetail(caseKey, c);
                } else {
                  cancelCaseDetail(caseKey);
                }
              }}
            >
              <summary>
                <span className="case__title">
                  {c.timestamp} {c.oracle}
                </span>
                <span className="case__toggle" aria-hidden="true" />
                {cid && <span className="pill">{cid}</span>}
                {c.flaky && <span className="pill pill--flaky">flaky</span>}
                {reasonLabel !== "other" && <span className="pill">{reasonLabel.replace(/_/g, " ")}</span>}
                {minimizeStatus && <span className="pill">minimize {minimizeStatus}</span>}
                {(expectedRowsTruncated || actualRowsTruncated) && (
                  <span className="pill pill--warn">
                    {expectedRowsTruncated && actualRowsTruncated
                      ? "rows truncated"
                      : expectedRowsTruncated
                      ? "expected rows truncated"
                      : "actual rows truncated"}
                  </span>
                )}
                {c.tidb_commit && <span className="pill">commit {c.tidb_commit.slice(0, 10)}</span>}
                {c.tidb_version && <span className="pill">{c.tidb_version.split("\n")[0]}</span>}
                {c.plan_signature && <span className="pill">plan {c.plan_signature.slice(0, 10)}</span>}
                {c.plan_signature_format && <span className="pill">{c.plan_signature_format}</span>}
                {metaLabelPreview.map((label) => (
                  <span className="pill pill--meta" key={`${cid || "case"}-label-${label}`}>
                    tag {label}
                  </span>
                ))}
                {metaLabelExtra > 0 && (
                  <span className="pill pill--meta" key={`${cid || "case"}-label-more`}>
                    +{metaLabelExtra}
                  </span>
                )}
                {metaIssue && (
                  <span className="pill pill--issue" key={`${cid || "case"}-issue`}>
                    issue {metaIssueDisplay}
                  </span>
                )}
              </summary>
              {isExpanded && (
                <div className="case__grid">
                {!detailLoaded && detailLoading && <div className="hint">Loading case details...</div>}
                {!detailLoaded && detailError && <div className="error">{detailError}</div>}
                {(downloadURL || similarURL) && (
                  <div className="case__actions">
                    {downloadURL && (
                      <a className="action-link" href={downloadURL} rel="noreferrer" download>
                        Download case
                      </a>
                    )}
                    {similarURL && (
                      <a className="action-link" href={similarURL} target="_blank" rel="noreferrer">
                        Open similar API
                      </a>
                    )}
                    {workerBaseURL && cid && (
                      <button
                        className="action-link action-link--button"
                        type="button"
                        onClick={() => void loadSimilarCases(cid)}
                        disabled={similarLoading}
                      >
                        {similarLoading ? "Loading similar..." : "Find similar in page"}
                      </button>
                    )}
                  </div>
                )}
                {(similarError || similarAnswer || similarList.length > 0) && (
                  <div className="similar-panel">
                    <LabelRow label="Similar bugs" />
                    {similarError && <div className="error">{similarError}</div>}
                    {similarAnswer && <p className="similar-panel__answer">{similarAnswer}</p>}
                    {similarList.length > 0 && (
                      <div className="similar-panel__list">
                        {similarList.map((item, itemIdx) => {
                          const sid = (item.case_id || "").trim();
                          const score = typeof item.similarity_score === "number" ? item.similarity_score : null;
                          const itemArchiveURLRaw = (item.archive_url || "").trim();
                          const itemReportURLRaw = (item.report_url || "").trim();
                          const itemArchiveURL = isHTTPURL(itemArchiveURLRaw) ? itemArchiveURLRaw : "";
                          const itemReportURL = isHTTPURL(itemReportURLRaw) ? itemReportURLRaw : "";
                          return (
                            <div className="similar-panel__item" key={`${sid || "similar"}-${itemIdx}`}>
                              <div className="similar-panel__meta">
                                <span className="pill">{sid || `#${itemIdx + 1}`}</span>
                                {item.oracle && <span className="pill">{item.oracle}</span>}
                                {item.error_reason && <span className="pill">{item.error_reason}</span>}
                                {item.error_type && <span className="pill">{item.error_type}</span>}
                                {score !== null && <span className="pill">score {score.toFixed(3)}</span>}
                              </div>
                              <div className="similar-panel__actions">
                                {itemArchiveURL && (
                                  <a className="action-link" href={itemArchiveURL} target="_blank" rel="noreferrer">
                                    Archive
                                  </a>
                                )}
                                {itemReportURL && (
                                  <a className="action-link" href={itemReportURL} target="_blank" rel="noreferrer">
                                    Report
                                  </a>
                                )}
                                {workerBaseURL && sid && (
                                  <a
                                    className="action-link"
                                    href={`${workerBaseURL}/api/v1/cases/${encodeURIComponent(sid)}/similar?limit=20&ai=1`}
                                    target="_blank"
                                    rel="noreferrer"
                                  >
                                    More like this
                                  </a>
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                )}
                <div className="case__meta">
                  {((workerBaseURL && cid) || metaLabels.length > 0 || Boolean(metaIssue)) && (() => {
                    const currentMeta = meta || emptyCaseMeta();
                    const issueLink = metaIssue ? issueLinkFrom(metaIssue) : null;
                    const showMetaError =
                      currentMeta.error && !(currentMeta.error === "unauthorized" && !workerToken.trim());
                    return (
                      <div className="case__meta-block">
                        <LabelRow label="Tags & Issue" />
                        {workerBaseURL && cid && currentMeta.loading && <div className="hint">Loading metadata...</div>}
                        {workerBaseURL && cid && showMetaError && <div className="error">{currentMeta.error}</div>}
                        {metaLabels.length > 0 && (
                          <div className="pill-row">
                            {metaLabels.map((label) => (
                              <span className="pill" key={`${cid}-${label}`}>{label}</span>
                            ))}
                          </div>
                        )}
                        {metaIssue && issueLink && (
                          <a className="action-link" href={issueLink.href} target="_blank" rel="noreferrer">
                            Issue {issueLink.label}
                          </a>
                        )}
                        {metaIssue && !issueLink && (
                          <div className="linked-issue">
                            <span className="pill">issue</span>
                            <span>{metaIssue}</span>
                          </div>
                        )}
                        {workerBaseURL && cid && (
                          <button
                            className="copy-btn"
                            type="button"
                            onClick={() => void openMetaEditor(cid)}
                          >
                            Edit tags & issue
                          </button>
                        )}
                      </div>
                    );
                  })()}
                  {c.error && (
                    <>
                      <LabelRow label="Error" onCopy={() => copyText("error", c.error || "")} />
                      <pre>{c.error}</pre>
                    </>
                  )}
                  {norecPredicate && (
                    <>
                      <LabelRow label="NoREC Predicate" onCopy={() => copyText("norec predicate", norecPredicate)} />
                      <pre>{norecPredicate}</pre>
                    </>
                  )}
                  {replaySQLBlock && (
                    <>
                      <LabelRow
                        label={replaySQLBlock.label}
                        onCopy={replaySQLBlock.copyText ? () => copyText(replaySQLBlock.label, replaySQLBlock.copyText || "") : undefined}
                      />
                      {replaySQLBlock.content}
                    </>
                  )}
                  {(() => {
                    const schemaFile = c.files?.["schema.sql"];
                    if (schemaFile?.content) {
                      const label = schemaFile.truncated ? `${schemaFile.name} (truncated)` : schemaFile.name || "schema.sql";
                      return (
                        <details className="fold" key="schema.sql" open={false}>
                          <summary>
                            <div className="fold__summary">
                              <span className="fold__icon" aria-hidden="true" />
                              <LabelRow label={label} onCopy={() => copyText(label, schemaFile.content || "")} />
                            </div>
                          </summary>
                          <pre>{schemaFile.content || ""}</pre>
                        </details>
                      );
                    }
                    return null;
                  })()}
                  {(() => {
                    const files = c.files || {};
                    const dataFile = files["data.tsv"];
                    if (dataFile?.content) {
                      const insertSQL = files["inserts.sql"]?.content || "";
                      const label = dataFile.truncated ? `${dataFile.name} (truncated)` : dataFile.name || "data";
                      return (
                        <details className="fold" key="data.tsv" open={false}>
                          <summary>
                            <div className="fold__summary">
                              <span className="fold__icon" aria-hidden="true" />
                              <LabelRow label="data" onCopy={() => copyText(label, dataFile.content || "")} />
                              {insertSQL && (
                                <button
                                  className="copy-btn"
                                  type="button"
                                  onClick={(e) => {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    copyText("inserts.sql", insertSQL);
                                  }}
                                >
                                  Copy inserts
                                </button>
                              )}
                            </div>
                          </summary>
                          <pre>{dataFile.content}</pre>
                        </details>
                      );
                    }
                    return null;
                  })()}
                  {(() => {
                    const files = c.files || {};
                    const reportFile = files["report.json"];
                    if (reportFile?.content) {
                      const baseName = reportFile.name || "report.json";
                      const label = reportFile.truncated ? `${baseName} (truncated)` : baseName;
                      return (
                        <details className="fold" key="report.json" open={false}>
                          <summary>
                            <div className="fold__summary">
                              <span className="fold__icon" aria-hidden="true" />
                              <LabelRow label={label} onCopy={() => copyText(label, reportFile.content || "")} />
                            </div>
                          </summary>
                          <pre>{reportFile.content || ""}</pre>
                        </details>
                      );
                    }
                    return null;
                  })()}
                  {(() => {
                    const caseFile = c.files?.["case.sql"];
                    if (caseFile?.content) {
                      return (
                        <>
                          <LabelRow label="case.sql" onCopy={() => copyText("case.sql", caseFile.content || "")} />
                          <pre>{formatSQL(caseFile.content)}</pre>
                        </>
                      );
                    }
                    return null;
                  })()}
                  {Object.keys(c.files || {}).map((key) => {
                    if (reservedFileKeys.has(key)) return null;
                    const f = c.files[key];
                    if (!f?.content) return null;
                    const fileName = (f.name || key).trim();
                    if (fileName === "case.tar.zst" || key === "case.tar.zst") return null;
                    if (archiveName && (key === archiveName || fileName === archiveName)) return null;
                    const label = f.truncated ? `${f.name} (truncated)` : f.name;
                    return (
                      <div key={key}>
                        <LabelRow label={label} onCopy={() => copyText(label, f.content || "")} />
                        <pre>{f.content}</pre>
                      </div>
                    );
                  })}
                </div>
                <div className="case__pair">
                  {pairedRows.map((row, i) => (
                    <div className="case__row" key={`case-row-${i}`}>
                      {renderBlock(row.left)}
                      {renderBlock(row.right)}
                    </div>
                  ))}
                </div>
                {(expectedExplainBlock || actualExplainBlock) && (
                  <div className="case__pair">
                    <div className="case__row">
                      {renderBlock(expectedExplainBlock)}
                      {renderBlock(actualExplainBlock)}
                    </div>
                  </div>
                )}
                {(optimizedExplainBlock || unoptimizedExplainBlock) && (
                  <div className="case__pair">
                    <div className="case__row">
                      {renderBlock(optimizedExplainBlock)}
                      {renderBlock(unoptimizedExplainBlock)}
                    </div>
                  </div>
                )}
                {diffBlocks.length > 0 && (
                  <div className="case__diffs">
                    {diffBlocks.map((block, idx) => (
                      <div className="case__block" key={`${block.label}-${idx}`}>
                        <LabelRow
                          label={block.label}
                          onCopy={block.copyText ? () => copyText(block.label, block.copyText || "") : undefined}
                        />
                        {block.content}
                      </div>
                    ))}
                  </div>
                )}
                </div>
              )}
            </details>
          );
        })}
      </main>
      {workerBaseURL && activeMetaID && (() => {
        const meta = caseMetaByID[activeMetaID] || emptyCaseMeta();
        const labelList = parseLabelInput(meta.draftLabels);
        const issueLink = meta.draftIssue ? issueLinkFrom(meta.draftIssue) : null;
        return (
          <div
            className="modal__backdrop"
            role="dialog"
            aria-modal="true"
            onClick={() => setActiveMetaID(null)}
          >
            <div className="modal" onClick={(event) => event.stopPropagation()}>
              <div className="modal__header">
                <div>
                  <div className="modal__title">Edit Tags & Issue</div>
                  <div className="modal__subtitle">{activeMetaID}</div>
                </div>
                <button className="copy-btn" type="button" onClick={() => setActiveMetaID(null)}>
                  Close
                </button>
              </div>
              {meta.loading && <div className="hint">Loading metadata...</div>}
              {meta.error && <div className="error">{meta.error}</div>}
              <div className="modal__section">
                <LabelRow label="Labels" />
                <div className="label-preset">
                  {presetLabels.map((label) => {
                    const active = labelList.some((item) => item.toLowerCase() === label.toLowerCase());
                    return (
                      <button
                        key={`preset-${label}`}
                        className={`label-chip${active ? " label-chip--active" : ""}`}
                        type="button"
                        onClick={() => {
                          updateCaseMetaState(activeMetaID, (state) => ({
                            ...state,
                            draftLabels: togglePresetLabel(state.draftLabels, label),
                          }));
                        }}
                      >
                        {label}
                      </button>
                    );
                  })}
                </div>
                {labelList.length > 0 && (
                  <div className="label-list">
                    {labelList.map((label) => (
                      <span className="label-chip label-chip--active" key={`label-${label}`}>
                        {label}
                        <button
                          type="button"
                          className="label-chip__remove"
                          onClick={() => {
                            updateCaseMetaState(activeMetaID, (state) => ({
                              ...state,
                              draftLabels: togglePresetLabel(state.draftLabels, label),
                            }));
                          }}
                        >
                          ×
                        </button>
                      </span>
                    ))}
                  </div>
                )}
                <div className="label-add">
                  <input
                    type="text"
                    placeholder="Add custom label"
                    value={meta.newLabel}
                    onChange={(e) => {
                      const value = e.target.value;
                      updateCaseMetaState(activeMetaID, (state) => ({ ...state, newLabel: value }));
                    }}
                  />
                  <button
                    className="action-link action-link--button"
                    type="button"
                    onClick={() => {
                      const value = meta.newLabel.trim();
                      if (!value) return;
                      updateCaseMetaState(activeMetaID, (state) => ({
                        ...state,
                        draftLabels: togglePresetLabel(state.draftLabels, value),
                        newLabel: "",
                      }));
                    }}
                  >
                    Add
                  </button>
                </div>
              </div>
              <div className="modal__section">
                <LabelRow label="Issue" />
                <input
                  type="text"
                  placeholder="Issue URL, repo#id, or #id"
                  value={meta.draftIssue}
                  onChange={(e) => {
                    const value = e.target.value;
                    updateCaseMetaState(activeMetaID, (state) => ({ ...state, draftIssue: value }));
                  }}
                />
                {issueLink && (
                  <a className="action-link" href={issueLink.href} target="_blank" rel="noreferrer">
                    Open {issueLink.label}
                  </a>
                )}
              </div>
              <div className="modal__actions">
                <button className="copy-btn" type="button" onClick={() => setActiveMetaID(null)}>
                  Cancel
                </button>
                <button
                  className="action-link action-link--button"
                  type="button"
                  onClick={() => void saveCaseMeta(activeMetaID)}
                  disabled={meta.saving || meta.loading}
                >
                  {meta.saving ? "Saving..." : "Save"}
                </button>
              </div>
            </div>
          </div>
        );
      })()}
    </div>
  );
}
