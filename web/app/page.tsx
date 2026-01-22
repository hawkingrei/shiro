"use client";

import { useEffect, useMemo, useState } from "react";
import { format } from "sql-formatter";

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
  plan_signature: string;
  plan_signature_format: string;
  expected: string;
  actual: string;
  error: string;
  norec_optimized_sql: string;
  norec_unoptimized_sql: string;
  norec_predicate: string;
  case_dir: string;
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

const detailString = (details: Record<string, unknown> | null, key: string): string => {
  if (!details) return "";
  const value = details[key];
  return typeof value === "string" ? value : "";
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

const reasonForCase = (c: CaseEntry): string => {
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
  const [query, setQuery] = useState("");
  const [oracle, setOracle] = useState("");
  const [commit, setCommit] = useState("");
  const [planSig, setPlanSig] = useState("");
  const [planSigFormat, setPlanSigFormat] = useState("");
  const [onlyErrors, setOnlyErrors] = useState(false);
  const [reason, setReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch("./report.json", { cache: "no-cache" })
      .then((res) => {
        if (!res.ok) {
          throw new Error(`failed to load report.json: ${res.status}`);
        }
        return res.json();
      })
      .then((data: ReportPayload) => {
        setPayload(data);
      })
      .catch((err: Error) => {
        setError(err.message);
      });
  }, []);

  const cases = payload?.cases ?? [];
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

  // TODO(hawkingrei): add a summary panel for reason counts (e.g., cache miss vs mismatch).
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

  if (error) {
    return <div className="page"><div className="error">Failed to load report.json: {error}</div></div>;
  }

  return (
    <div className="page">
      <header className="hero">
        <div className="hero__text">
          <div className="hero__kicker">Shiro Fuzzing</div>
          <h1>Case Report Index</h1>
          <p className="hero__sub">
            Static frontend reading <code>report.json</code>. Deploy to GitHub Pages or Vercel and update the JSON to refresh.
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
            <label className="toggle">
              <input type="checkbox" checked={onlyErrors} onChange={(e) => setOnlyErrors(e.target.checked)} />
              Only errors
            </label>
          </div>
          <div className="stats">Total: {cases.length} | Showing: {filtered.length}</div>
        </div>
      </header>

      <main className="cases">
        {filtered.map((c, idx) => {
          const reasonLabel = reasonForCase(c);
          const expectedSQL = detailString(c.details, "replay_expected_sql") || c.norec_optimized_sql || "";
          const actualSQL = detailString(c.details, "replay_actual_sql") || c.norec_unoptimized_sql || "";
          const norecPredicate = c.norec_predicate || "";
          const unoptimizedExplain = detailString(c.details, "unoptimized_explain");
          const optimizedExplain = detailString(c.details, "optimized_explain");
          return (
            <details className="case" key={c.id || idx}>
              <summary>
                <span className="case__title">
                  {c.timestamp} {c.oracle}
                </span>
                <span className="case__toggle" aria-hidden="true" />
                {c.case_dir && <span className="pill">{c.case_dir}</span>}
                {reasonLabel !== "other" && <span className="pill">{reasonLabel.replace(/_/g, " ")}</span>}
                {c.tidb_commit && <span className="pill">commit {c.tidb_commit.slice(0, 10)}</span>}
                {c.tidb_version && <span className="pill">{c.tidb_version.split("\n")[0]}</span>}
                {c.plan_signature && <span className="pill">plan {c.plan_signature.slice(0, 10)}</span>}
                {c.plan_signature_format && <span className="pill">{c.plan_signature_format}</span>}
              </summary>
              <div className="case__grid">
                <div>
                  <LabelRow label="Expected" onCopy={() => copyText("expected", c.expected || "")} />
                  <pre>{c.expected || ""}</pre>
                  {expectedSQL && (
                    <>
                      <LabelRow label="Expected SQL" onCopy={() => copyText("expected sql", expectedSQL)} />
                      <pre>{formatSQL(expectedSQL)}</pre>
                    </>
                  )}
                  {optimizedExplain && (
                    <>
                      <LabelRow label="Optimized EXPLAIN" onCopy={() => copyText("optimized explain", optimizedExplain)} />
                      <pre>{optimizedExplain}</pre>
                    </>
                  )}
                </div>
                <div>
                  <LabelRow label="Actual" onCopy={() => copyText("actual", c.actual || "")} />
                  <pre>{c.actual || ""}</pre>
                  {actualSQL && (
                    <>
                      <LabelRow label="Actual SQL" onCopy={() => copyText("actual sql", actualSQL)} />
                      <pre>{formatSQL(actualSQL)}</pre>
                    </>
                  )}
                  {unoptimizedExplain && (
                    <>
                      <LabelRow label="Unoptimized EXPLAIN" onCopy={() => copyText("unoptimized explain", unoptimizedExplain)} />
                      <pre>{unoptimizedExplain}</pre>
                    </>
                  )}
                  {c.error && (
                    <>
                      <LabelRow label="Error" onCopy={() => copyText("error", c.error || "")} />
                      <pre>{c.error}</pre>
                    </>
                  )}
                </div>
                <div>
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
                  {Object.keys(c.files || {}).map((key) => {
                    if (key === "case.sql") return null;
                    if (key === "inserts.sql") return null;
                    if (key === "plan_replayer.zip") return null;
                    const f = c.files[key];
                    if (!f?.content) return null;
                    const label = f.truncated ? `${f.name} (truncated)` : f.name;
                    if (key === "data.tsv") {
                      const insertSQL = c.files?.["inserts.sql"]?.content || "";
                      return (
                        <details className="fold" key={key} open={false}>
                          <summary>
                            <div className="fold__summary">
                              <span className="fold__icon" aria-hidden="true" />
                              <LabelRow label="data" onCopy={() => copyText(label, f.content || "")} />
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
                          <pre>{f.content}</pre>
                        </details>
                      );
                    }
                    return (
                      <div key={key}>
                        <LabelRow label={label} onCopy={() => copyText(label, f.content || "")} />
                        <pre>{f.content}</pre>
                      </div>
                    );
                  })}
                </div>
              </div>
            </details>
          );
        })}
      </main>
    </div>
  );
}
