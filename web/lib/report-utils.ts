export type CaseLike = {
  id?: string;
  case_id?: string;
  case_dir?: string;
  upload_location?: string;
  archive_name?: string;
  archive_url?: string;
  report_url?: string;
};

export const isHTTPURL = (value: string): boolean => {
  const normalized = (value || "").trim().toLowerCase();
  return normalized.startsWith("http://") || normalized.startsWith("https://");
};

export const objectURL = (base: string, name: string): string => {
  const trimmedBase = (base || "").trim().replace(/\/+$/, "");
  const trimmedName = (name || "").trim().replace(/^\/+/, "");
  if (!trimmedBase || !trimmedName) return "";
  return `${trimmedBase}/${trimmedName}`;
};

export const caseID = (c: CaseLike): string => {
  return (c.case_id || c.case_dir || c.id || "").trim();
};

export const caseArchiveURL = (c: CaseLike): string => {
  const explicit = (c.archive_url || "").trim();
  if (isHTTPURL(explicit)) return explicit;
  const base = (c.upload_location || "").trim();
  if (!isHTTPURL(base)) return "";
  return objectURL(base, c.archive_name || "");
};

export const caseReportURL = (c: CaseLike): string => {
  const explicit = (c.report_url || "").trim();
  if (isHTTPURL(explicit)) return explicit;
  const base = (c.upload_location || "").trim();
  if (!isHTTPURL(base)) return "";
  return objectURL(base, "report.json");
};

export const similarCasesURL = (workerBaseURL: string, c: CaseLike): string => {
  const cid = caseID(c);
  const base = (workerBaseURL || "").trim().replace(/\/+$/, "");
  if (!base || !cid) return "";
  return `${base}/api/v1/cases/${encodeURIComponent(cid)}/similar?limit=20&ai=1`;
};

export const workerDownloadURL = (workerBaseURL: string, c: CaseLike): string => {
  const cid = caseID(c);
  const base = (workerBaseURL || "").trim().replace(/\/+$/, "");
  if (!base || !cid) return "";
  return `${base}/api/v1/cases/${encodeURIComponent(cid)}/download`;
};
