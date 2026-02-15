CREATE TABLE IF NOT EXISTS cases (
  case_id TEXT PRIMARY KEY,
  labels_json TEXT NOT NULL DEFAULT '[]',
  linked_issue TEXT NOT NULL DEFAULT ''
);
