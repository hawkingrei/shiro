"use client";

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { format } from "sql-formatter";
import ReactDiffViewer, { DiffMethod } from "react-diff-viewer-continued";
import {
  caseArchiveURL,
  caseID,
  isHTTPURL,
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

export default function Page() {
  const [payload, setPayload] = useState<ReportPayload | null>(null);
  const [similarByCase, setSimilarByCase] = useState<Record<string, SimilarPayload>>({});
  const [similarLoadingByCase, setSimilarLoadingByCase] = useState<Record<string, boolean>>({});
  const [similarErrorByCase, setSimilarErrorByCase] = useState<Record<string, string>>({});
  const [query, setQuery] = useState("");
  const [oracle, setOracle] = useState("");
  const [commit, setCommit] = useState("");
  const [planSig, setPlanSig] = useState("");
  const [planSigFormat, setPlanSigFormat] = useState("");
  const [onlyErrors, setOnlyErrors] = useState(false);
  const [showExplainSame, setShowExplainSame] = useState(false);
  const [reason, setReason] = useState("");
  const [labelFilter, setLabelFilter] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [workerBaseURL, setWorkerBaseURL] = useState(workerBaseURLEnv);
  const [workerToken, setWorkerToken] = useState("");
  const [caseMetaByID, setCaseMetaByID] = useState<Record<string, CaseMetaState>>({});
  const [activeMetaID, setActiveMetaID] = useState<string | null>(null);

  useEffect(() => {
    const stored = window.sessionStorage.getItem(workerTokenStorageKey) || "";
    if (stored) {
      setWorkerToken(stored);
    }
  }, []);

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
    let canceled = false;
    const load = async () => {
      let lastErr: Error | null = null;
      const urls: string[] = [];
      if (reportsBaseURL) {
        urls.push(`${reportsBaseURL}/reports.json`, `${reportsBaseURL}/report.json`);
      }
      urls.push("./reports.json", "./report.json");
      for (const url of urls) {
        try {
          const res = await fetch(url, { cache: "no-cache" });
          if (!res.ok) {
            throw new Error(`failed to load ${url}: ${res.status}`);
          }
          const data: ReportPayload = await res.json();
          if (!canceled) {
            setPayload(data);
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
      const headers: Record<string, string> = {};
      if (workerToken.trim()) {
        headers.Authorization = `Bearer ${workerToken.trim()}`;
      }
      const resp = await fetch(`${workerBaseURL}/api/v1/cases/${encodeURIComponent(caseID)}`, {
        cache: "no-cache",
        headers,
      });
      if (resp.status === 404) {
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          loading: false,
          loaded: true,
          error: "metadata not found",
        }));
        return;
      }
      if (resp.status === 401) {
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          loading: false,
          loaded: false,
          error: "unauthorized",
        }));
        return;
      }
      if (!resp.ok) {
        updateCaseMetaState(caseID, (state) => ({
          ...state,
          loading: false,
          loaded: false,
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
        loaded: false,
        error: "load failed",
      }));
    }
  };

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
    if (current.loading || !current.loaded) {
      updateCaseMetaState(caseID, (state) => ({
        ...state,
        error: "metadata not loaded",
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
          Authorization: `Bearer ${workerToken}`,
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

  const cases = useMemo(() => payload?.cases ?? [], [payload]);
  const q = query.trim().toLowerCase();

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
    return Array.from(new Set(cases.map((c) => reasonForCase(c)))).sort();
  }, [cases]);

  const labelOptions = useMemo(() => {
    const labels = new Set<string>();
    cases.forEach((c) => {
      const cid = caseID(c);
      if (!cid) return;
      const meta = caseMetaByID[cid];
      if (!meta?.loaded) return;
      meta.labels.forEach((label) => {
        if (label) labels.add(label);
      });
    });
    return Array.from(labels.values()).sort();
  }, [cases, caseMetaByID]);

  const filtered = useMemo(() => {
    return cases.filter((c) => {
      if (oracle && c.oracle !== oracle) return false;
      if (commit && c.tidb_commit !== commit) return false;
      if (planSig) {
        const cand = (c.plan_signature || "").trim().toLowerCase();
        const target = planSig.trim().toLowerCase();
        if (cand !== target) return false;
      }
      if (planSigFormat && c.plan_signature_format !== planSigFormat) return false;
      if (onlyErrors && !c.error) return false;
      if (reason && reasonForCase(c) !== reason) return false;
      if (labelFilter) {
        const cid = caseID(c);
        if (!cid) return false;
        const meta = caseMetaByID[cid];
        if (!meta?.loaded) return false;
        const match = meta.labels.some((label) => label.toLowerCase() === labelFilter.toLowerCase());
        if (!match) return false;
      }
      if (!q) return true;
      const hay = [
        c.oracle,
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
        ...(c.sql || []),
        JSON.stringify(c.details || {}),
      ]
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [cases, oracle, commit, planSig, planSigFormat, onlyErrors, reason, q]);

  const loadSimilarCases = async (cid: string) => {
    const caseIDValue = (cid || "").trim();
    if (!workerBaseURL || !caseIDValue) return;
    setSimilarLoadingByCase((prev) => ({ ...prev, [caseIDValue]: true }));
    setSimilarErrorByCase((prev) => ({ ...prev, [caseIDValue]: "" }));
    try {
      const url = `${workerBaseURL}/api/v1/cases/${encodeURIComponent(caseIDValue)}/similar?limit=20&ai=1`;
      const res = await fetch(url, { cache: "no-cache" });
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
    filtered.forEach((c) => {
      const reasonLabel = reasonForCase(c);
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

  if (error) {
    return <div className="page"><div className="error">Failed to load reports.json/report.json: {error}</div></div>;
  }

  return (
    <div className="page">
      <header className="hero">
        <div className="hero__text">
          <div className="hero__kicker">Shiro Fuzzing</div>
          <h1>Case Report Index</h1>
          <p className="hero__sub">
            Static frontend reading <code>reports.json</code> with fallback to <code>report.json</code>. Deploy to GitHub Pages or Vercel and update the JSON to refresh. Set <code>NEXT_PUBLIC_REPORTS_BASE_URL</code> to load JSON from a public bucket or CDN.
          </p>
          <div className="hero__meta">
            <span>Generated: {payload?.generated_at ?? "-"}</span>
            <span>Source: {payload?.source ?? "-"}</span>
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
          <div className="stats">Total: {cases.length} | Showing: {filtered.length}</div>
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

      <main className="cases">
        {filtered.map((c, idx) => {
          const cid = caseID(c);
          const meta = cid ? (caseMetaByID[cid] || emptyCaseMeta()) : null;
          const metaLabels = meta?.loaded ? meta.labels : [];
          const metaLabelPreview = metaLabels.slice(0, 3);
          const metaLabelExtra = metaLabels.length - metaLabelPreview.length;
          const metaIssue = meta?.loaded ? meta.linkedIssue : "";
          const metaIssueDisplay = metaIssue.length > 32 ? `${metaIssue.slice(0, 32)}...` : metaIssue;
          const archiveURL = caseArchiveURL(c);
          const downloadURL = archiveURL;
          const archiveName = (c.archive_name || "").trim();
          const similarURL = similarCasesURL(workerBaseURL, c);
          const similarPayload = cid ? similarByCase[cid] : undefined;
          const similarList = similarPayload?.matches || [];
          const similarAnswer = (similarPayload?.answer || "").trim();
          const similarLoading = cid ? Boolean(similarLoadingByCase[cid]) : false;
          const similarError = cid ? (similarErrorByCase[cid] || "").trim() : "";
          const reasonLabel = reasonForCase(c);
          const expectedSQL = detailString(c.details, "replay_expected_sql") || c.norec_optimized_sql || "";
          const actualSQL = detailString(c.details, "replay_actual_sql") || c.norec_unoptimized_sql || "";
          const norecPredicate = c.norec_predicate || "";
          const expectedRowsTruncated = detailBool(c.details, "expected_rows_truncated");
          const actualRowsTruncated = detailBool(c.details, "actual_rows_truncated");
          const expectedExplainRaw = detailString(c.details, "expected_explain");
          const actualExplainRaw = detailString(c.details, "actual_explain");
          const unoptimizedExplainRaw = detailString(c.details, "unoptimized_explain");
          const optimizedExplainRaw = detailString(c.details, "optimized_explain");
          const expectedExplain = formatExplain(expectedExplainRaw);
          const actualExplain = formatExplain(actualExplainRaw);
          const optimizedExplain = formatExplain(optimizedExplainRaw);
          const unoptimizedExplain = formatExplain(unoptimizedExplainRaw);
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
              key={c.id || cid || idx}
              onToggle={(event) => {
                if ((event.currentTarget as HTMLDetailsElement).open && cid) {
                  void ensureCaseMeta(cid);
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
              <div className="case__grid">
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
                  {workerBaseURL && cid && (() => {
                    const issueLink = meta.linkedIssue ? issueLinkFrom(meta.linkedIssue) : null;
                    return (
                      <div className="case__meta-block">
                        <LabelRow label="Tags & Issue" />
                        {meta.loading && <div className="hint">Loading metadata...</div>}
                        {meta.error && <div className="error">{meta.error}</div>}
                        {meta.labels.length > 0 && (
                          <div className="pill-row">
                            {meta.labels.map((label) => (
                              <span className="pill" key={`${cid}-${label}`}>{label}</span>
                            ))}
                          </div>
                        )}
                        {meta.linkedIssue && issueLink && (
                          <a className="action-link" href={issueLink.href} target="_blank" rel="noreferrer">
                            Issue {issueLink.label}
                          </a>
                        )}
                        {meta.linkedIssue && !issueLink && (
                          <div className="linked-issue">
                            <span className="pill">issue</span>
                            <span>{meta.linkedIssue}</span>
                          </div>
                        )}
                        <button
                          className="copy-btn"
                          type="button"
                          onClick={() => void openMetaEditor(cid)}
                        >
                          Edit tags & issue
                        </button>
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
                  disabled={meta.saving || meta.loading || !meta.loaded}
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
