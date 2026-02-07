CREATE TABLE IF NOT EXISTS cases (
  case_id TEXT PRIMARY KEY,
  oracle TEXT NOT NULL DEFAULT '',
  timestamp TEXT NOT NULL DEFAULT '',
  error_reason TEXT NOT NULL DEFAULT '',
  error_type TEXT NOT NULL DEFAULT '',
  error_text TEXT NOT NULL DEFAULT '',
  false_positive INTEGER NOT NULL DEFAULT 0,
  linked_issue TEXT NOT NULL DEFAULT '',
  labels_json TEXT NOT NULL DEFAULT '[]',
  upload_location TEXT NOT NULL DEFAULT '',
  report_url TEXT NOT NULL DEFAULT '',
  archive_url TEXT NOT NULL DEFAULT '',
  manifest_url TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cases_timestamp ON cases(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_cases_oracle ON cases(oracle);
CREATE INDEX IF NOT EXISTS idx_cases_error_reason ON cases(error_reason);
CREATE INDEX IF NOT EXISTS idx_cases_error_type ON cases(error_type);
CREATE INDEX IF NOT EXISTS idx_cases_false_positive ON cases(false_positive);
