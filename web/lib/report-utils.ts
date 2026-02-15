export type CaseLike = {
  id?: string;
  case_id?: string;
  case_dir?: string;
  dir?: string;
  upload_location?: string;
  archive_name?: string;
  archive_url?: string;
  report_url?: string;
};

const GCS_PUBLIC_BASE_URL = "https://storage.googleapis.com";

export const isHTTPURL = (value: string): boolean => {
  const normalized = (value || "").trim().toLowerCase();
  return normalized.startsWith("http://") || normalized.startsWith("https://");
};

export const isGCSURL = (value: string): boolean => {
  const normalized = (value || "").trim().toLowerCase();
  return normalized.startsWith("gs://");
};

const parseGCSURI = (uri: string): { bucket: string; key: string } | null => {
  const normalized = (uri || "").trim();
  if (!isGCSURL(normalized)) {
    return null;
  }
  const withoutScheme = normalized.slice("gs://".length).replace(/^\/+/, "");
  if (!withoutScheme) {
    return null;
  }
  const idx = withoutScheme.indexOf("/");
  if (idx < 0) {
    return { bucket: withoutScheme, key: "" };
  }
  const bucket = withoutScheme.slice(0, idx).trim();
  const key = withoutScheme.slice(idx + 1).replace(/^\/+|\/+$/g, "");
  if (!bucket) {
    return null;
  }
  return { bucket, key };
};

const gcsURLToPublic = (gsURL: string): string => {
  const parsed = parseGCSURI(gsURL);
  if (!parsed || !parsed.key) {
    return "";
  }
  return `${GCS_PUBLIC_BASE_URL}/${parsed.bucket}/${parsed.key}`;
};

const gcsObjectURL = (gsURL: string, objectName: string): string => {
  const parsed = parseGCSURI(gsURL);
  const name = (objectName || "").trim().replace(/^\/+/, "");
  if (!parsed || !name) {
    return "";
  }
  const key = parsed.key ? `${parsed.key}/${name}` : name;
  return `${GCS_PUBLIC_BASE_URL}/${parsed.bucket}/${key}`;
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
  if (isGCSURL(explicit)) return gcsURLToPublic(explicit);
  const base = (c.upload_location || c.dir || "").trim();
  if (isHTTPURL(base)) {
    return objectURL(base, c.archive_name || "");
  }
  if (isGCSURL(base)) {
    return gcsObjectURL(base, c.archive_name || "");
  }
  return "";
};

export const caseReportURL = (c: CaseLike): string => {
  const explicit = (c.report_url || "").trim();
  if (isHTTPURL(explicit)) return explicit;
  if (isGCSURL(explicit)) return gcsURLToPublic(explicit);
  const base = (c.upload_location || c.dir || "").trim();
  if (isHTTPURL(base)) {
    return objectURL(base, "report.json");
  }
  if (isGCSURL(base)) {
    return gcsObjectURL(base, "report.json");
  }
  return "";
};

export const similarCasesURL = (workerBaseURL: string, c: CaseLike): string => {
  const cid = caseID(c);
  const base = (workerBaseURL || "").trim().replace(/\/+$/, "");
  if (!base || !cid) return "";
  return `${base}/api/v1/cases/${encodeURIComponent(cid)}/similar?limit=20&ai=1`;
};
