export type CaseLike = {
  id?: string;
  case_id?: string;
  case_dir?: string;
  upload_location?: string;
  archive_name?: string;
  archive_url?: string;
  report_url?: string;
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
  if ((c.archive_url || "").trim()) return (c.archive_url || "").trim();
  return objectURL(c.upload_location || "", c.archive_name || "");
};

export const caseReportURL = (c: CaseLike): string => {
  if ((c.report_url || "").trim()) return (c.report_url || "").trim();
  return objectURL(c.upload_location || "", "report.json");
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
