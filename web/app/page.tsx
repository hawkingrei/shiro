"use client";

import { useEffect, useMemo, useState } from "react";

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
          return (
            <details className="case" key={c.id || idx}>
              <summary>
                <span className="case__title">
                  {c.timestamp} {c.oracle}
                </span>
                {reasonLabel !== "other" && <span className="pill">{reasonLabel.replace(/_/g, " ")}</span>}
                {c.tidb_commit && <span className="pill">commit {c.tidb_commit.slice(0, 10)}</span>}
                {c.tidb_version && <span className="pill">{c.tidb_version.split("\n")[0]}</span>}
                {c.plan_signature && <span className="pill">plan {c.plan_signature.slice(0, 10)}</span>}
                {c.plan_signature_format && <span className="pill">{c.plan_signature_format}</span>}
              </summary>
              <div className="case__grid">
                <div>
                  <div className="label">Expected</div>
                  <pre>{c.expected || ""}</pre>
                  <div className="label">Actual</div>
                  <pre>{c.actual || ""}</pre>
                  <div className="label">Error</div>
                  <pre>{c.error || ""}</pre>
                </div>
                <div>
                  <div className="label">SQL</div>
                  <pre>{(c.sql || []).join("\n\n")}</pre>
                </div>
                <div>
                  {Object.keys(c.files || {}).map((key) => {
                    const f = c.files[key];
                    if (!f?.content) return null;
                    const label = f.truncated ? `${f.name} (truncated)` : f.name;
                    return (
                      <div key={key}>
                        <div className="label">{label}</div>
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
