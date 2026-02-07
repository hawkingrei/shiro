"use client";

import { useEffect, useMemo, useState } from "react";
import { format } from "sql-formatter";
import ReactDiffViewer from "react-diff-viewer-continued";

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

const caseHasTruncation = (c: CaseEntry): boolean => {
  return detailBool(c.details, "expected_rows_truncated") || detailBool(c.details, "actual_rows_truncated");
};

const objectURL = (base: string, name: string): string => {
  const trimmedBase = (base || "").trim().replace(/\/+$/, "");
  const trimmedName = (name || "").trim().replace(/^\/+/, "");
  if (!trimmedBase || !trimmedName) return "";
  return `${trimmedBase}/${trimmedName}`;
};

const caseID = (c: CaseEntry): string => {
  return (c.case_id || c.case_dir || c.id || "").trim();
};

const caseArchiveURL = (c: CaseEntry): string => {
  if ((c.archive_url || "").trim()) return (c.archive_url || "").trim();
  return objectURL(c.upload_location || "", c.archive_name || "");
};

const caseReportURL = (c: CaseEntry): string => {
  if ((c.report_url || "").trim()) return (c.report_url || "").trim();
  return objectURL(c.upload_location || "", "report.json");
};

const workerBaseURL = (process.env.NEXT_PUBLIC_WORKER_BASE_URL || "").trim().replace(/\/+$/, "");

const similarCasesURL = (c: CaseEntry): string => {
  const cid = caseID(c);
  if (!workerBaseURL || !cid) return "";
  return `${workerBaseURL}/api/v1/cases/${encodeURIComponent(cid)}/similar?limit=20&ai=1`;
};

const workerDownloadURL = (c: CaseEntry): string => {
  const cid = caseID(c);
  if (!workerBaseURL || !cid) return "";
  return `${workerBaseURL}/api/v1/cases/${encodeURIComponent(cid)}/download`;
};

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

type CaseBlock = { label: string; content: JSX.Element; copyText?: string };

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
  const [viewMode, setViewMode] = useState<"list" | "waterfall">("list");
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let canceled = false;
    const load = async () => {
      let lastErr: Error | null = null;
      for (const url of ["./reports.json", "./report.json"]) {
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
      if (!q) return true;
      const hay = [
        c.oracle,
        c.error,
        c.expected,
        c.actual,
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
            Static frontend reading <code>reports.json</code> with fallback to <code>report.json</code>. Deploy to GitHub Pages or Vercel and update the JSON to refresh.
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
            <select value={viewMode} onChange={(e) => setViewMode(e.target.value as "list" | "waterfall")}>
              <option value="list">List view</option>
              <option value="waterfall">Waterfall view</option>
            </select>
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

      <main className={`cases ${viewMode === "waterfall" ? "cases--waterfall" : ""}`}>
        {filtered.map((c, idx) => {
          const cid = caseID(c);
          const archiveURL = caseArchiveURL(c);
          const workerArchiveURL = workerDownloadURL(c);
          const downloadURL = workerArchiveURL || archiveURL;
          const reportURL = caseReportURL(c);
          const similarURL = similarCasesURL(c);
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
          const expectedBlock: CaseBlock = {
            label: "Expected",
            content: <pre>{c.expected || ""}</pre>,
            copyText: c.expected || "",
          };
          const actualBlock: CaseBlock = {
            label: "Actual",
            content: <pre>{c.actual || ""}</pre>,
            copyText: c.actual || "",
          };
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
                      compareMethod="diffWordsWithSpace"
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
                      compareMethod="diffWordsWithSpace"
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
            <details className="case" key={c.id || cid || idx}>
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
              </summary>
              <div className="case__grid">
                {(downloadURL || reportURL || similarURL) && (
                  <div className="case__actions">
                    {downloadURL && (
                      <a className="action-link" href={downloadURL} target="_blank" rel="noreferrer">
                        Download archive
                      </a>
                    )}
                    {reportURL && (
                      <a className="action-link" href={reportURL} target="_blank" rel="noreferrer">
                        Open report.json
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
                          const itemArchiveURL = (item.archive_url || "").trim();
                          const itemReportURL = (item.report_url || "").trim();
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
                  {!expectedSQL && !actualSQL && c.files?.["case.sql"]?.content && (
                    <>
                      <LabelRow label="case.sql" onCopy={() => copyText("case.sql", c.files["case.sql"].content || "")} />
                      <pre>{formatSQL(c.files["case.sql"].content)}</pre>
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
                  {Object.keys(c.files || {}).map((key) => {
                    if (key === "case.sql") return null;
                    if (key === "inserts.sql") return null;
                    if (key === "plan_replayer.zip") return null;
                    if (key === "data.tsv") return null;
                    if (key === "schema.sql") return null;
                    const f = c.files[key];
                    if (!f?.content) return null;
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
    </div>
  );
}
