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

const isExplainTreeLine = (line: string): boolean => {
  if (!line) return false;
  if (/^[\s│├└┌┐─┬┼]+/.test(line)) return true;
  if (line.includes("->")) return true;
  return false;
};

const splitExplainLine = (line: string): string[] => {
  if (!line) return [""];
  if (line.includes("\t")) {
    const match = line.match(/^(\s*)(.*)$/);
    const prefix = match ? match[1] : "";
    const rest = match ? match[2] : line;
    const parts = rest.split(/\t+/).map((part) => part.trim());
    if (parts.length > 0) {
      parts[0] = prefix + parts[0];
    }
    return parts;
  }
  if (/\s{2,}/.test(line)) {
    const match = line.match(/^(\s*)(.*)$/);
    const prefix = match ? match[1] : "";
    const rest = match ? match[2] : line;
    const parts = rest.trim().split(/\s{2,}/);
    if (parts.length > 0) {
      parts[0] = prefix + parts[0];
    }
    return parts;
  }
  return [line.trim()];
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
  const [showExplainSame, setShowExplainSame] = useState(false);
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
            <label className="toggle">
              <input type="checkbox" checked={showExplainSame} onChange={(e) => setShowExplainSame(e.target.checked)} />
              Show EXPLAIN unchanged
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
                      showDiffOnly
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
                      showDiffOnly
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
                    {diffBlocks.map((block) => (
                      <div className="case__block" key={block.label}>
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
