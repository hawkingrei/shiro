import assert from "node:assert/strict";
import test from "node:test";

import {
  caseArchiveURL,
  caseID,
  caseReportURL,
  isGCSURL,
  isHTTPURL,
  objectURL,
  similarCasesURL,
  workerDownloadURL,
} from "../lib/report-utils";

test("objectURL trims slashes", () => {
  assert.equal(objectURL("https://example.com/a/", "/b.json"), "https://example.com/a/b.json");
  assert.equal(objectURL("", "x"), "");
  assert.equal(objectURL("https://example.com", ""), "");
});

test("caseID follows case_id -> case_dir -> id fallback", () => {
  assert.equal(caseID({ case_id: "id-1", case_dir: "dir-1", id: "id-2" }), "id-1");
  assert.equal(caseID({ case_dir: "dir-1", id: "id-2" }), "dir-1");
  assert.equal(caseID({ id: "id-2" }), "id-2");
});

test("caseArchiveURL and caseReportURL map http(s) and gs:// links", () => {
  assert.equal(
    caseArchiveURL({ upload_location: "https://cdn.example.com/abc/", archive_name: "case.tar.zst" }),
    "https://cdn.example.com/abc/case.tar.zst",
  );
  assert.equal(
    caseReportURL({ upload_location: "https://cdn.example.com/abc/" }),
    "https://cdn.example.com/abc/report.json",
  );
  assert.equal(
    caseArchiveURL({ upload_location: "s3://bucket/abc/", archive_name: "case.tar.zst" }),
    "",
  );
  assert.equal(
    caseReportURL({ upload_location: "s3://bucket/abc/" }),
    "",
  );
  assert.equal(
    caseArchiveURL({ upload_location: "gs://bucket/abc/", archive_name: "case.tar.zst" }),
    "https://storage.googleapis.com/bucket/abc/case.tar.zst",
  );
  assert.equal(
    caseReportURL({ upload_location: "gs://bucket/abc/" }),
    "https://storage.googleapis.com/bucket/abc/report.json",
  );
  assert.equal(
    caseArchiveURL({ archive_url: "gs://bucket/abc/case.tar.zst" }),
    "https://storage.googleapis.com/bucket/abc/case.tar.zst",
  );
  assert.equal(
    caseReportURL({ report_url: "gs://bucket/abc/report.json" }),
    "https://storage.googleapis.com/bucket/abc/report.json",
  );
  assert.equal(
    caseArchiveURL({ dir: "gs://bucket/abc/123", archive_name: "case.tar.zst" }),
    "https://storage.googleapis.com/bucket/abc/123/case.tar.zst",
  );
});

test("isHTTPURL validates link scheme", () => {
  assert.equal(isHTTPURL("https://example.com/a"), true);
  assert.equal(isHTTPURL("http://example.com/a"), true);
  assert.equal(isHTTPURL("s3://bucket/a"), false);
  assert.equal(isHTTPURL("gs://bucket/a"), false);
  assert.equal(isHTTPURL(""), false);
});

test("isGCSURL validates gs scheme", () => {
  assert.equal(isGCSURL("gs://bucket/a"), true);
  assert.equal(isGCSURL("gs://bucket"), true);
  assert.equal(isGCSURL("https://example.com/a"), false);
  assert.equal(isGCSURL(""), false);
});

test("similar and worker download URL generation", () => {
  const workerBase = "https://worker.example.com/";
  const c = { case_id: "0194d4f8-b6ce-7d4e-b13d-3be7446954d4" };
  assert.equal(
    similarCasesURL(workerBase, c),
    "https://worker.example.com/api/v1/cases/0194d4f8-b6ce-7d4e-b13d-3be7446954d4/similar?limit=20&ai=1",
  );
  assert.equal(
    workerDownloadURL(workerBase, c),
    "https://worker.example.com/api/v1/cases/0194d4f8-b6ce-7d4e-b13d-3be7446954d4/download",
  );
});
