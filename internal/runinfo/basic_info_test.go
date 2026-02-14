package runinfo

import "testing"

func TestFromEnvGitHubActions(t *testing.T) {
	clearKnownEnv(t)
	t.Setenv("GITHUB_ACTIONS", "true")
	t.Setenv("GITHUB_SERVER_URL", "https://github.com")
	t.Setenv("GITHUB_REPOSITORY", "hawkingrei/shiro")
	t.Setenv("GITHUB_HEAD_REF", "feature/env-meta")
	t.Setenv("GITHUB_REF", "refs/pull/108/merge")
	t.Setenv("GITHUB_SHA", "deadbeef")
	t.Setenv("GITHUB_WORKFLOW", "CI")
	t.Setenv("GITHUB_JOB", "test")
	t.Setenv("GITHUB_RUN_ID", "123456")
	t.Setenv("GITHUB_RUN_NUMBER", "42")
	t.Setenv("GITHUB_EVENT_NAME", "pull_request")
	t.Setenv("GITHUB_ACTOR", "hawkingrei")

	info := FromEnv()
	if info == nil {
		t.Fatalf("expected run info")
	}
	if !info.CI {
		t.Fatalf("expected ci=true")
	}
	if info.Provider != "github_actions" {
		t.Fatalf("provider=%q", info.Provider)
	}
	if info.Repository != "hawkingrei/shiro" {
		t.Fatalf("repository=%q", info.Repository)
	}
	if info.Branch != "feature/env-meta" {
		t.Fatalf("branch=%q", info.Branch)
	}
	if info.PullRequest != "108" {
		t.Fatalf("pull_request=%q", info.PullRequest)
	}
	if info.BuildURL != "https://github.com/hawkingrei/shiro/actions/runs/123456" {
		t.Fatalf("build_url=%q", info.BuildURL)
	}
}

func TestFromEnvShiroOverrides(t *testing.T) {
	clearKnownEnv(t)
	t.Setenv("SHIRO_CI_PROVIDER", "manual")
	t.Setenv("SHIRO_CI_REPOSITORY", "hawkingrei/shiro")
	t.Setenv("SHIRO_CI_BRANCH", "nightly")
	t.Setenv("SHIRO_CI_COMMIT", "abc123")
	t.Setenv("SHIRO_CI_WORKFLOW", "nightly-run")
	t.Setenv("SHIRO_CI_RUN_ID", "run-77")

	info := FromEnv()
	if info == nil {
		t.Fatalf("expected run info")
	}
	if !info.CI {
		t.Fatalf("expected ci=true when shiro overrides are set")
	}
	if info.Provider != "manual" {
		t.Fatalf("provider=%q", info.Provider)
	}
	if info.Branch != "nightly" {
		t.Fatalf("branch=%q", info.Branch)
	}
	if info.Commit != "abc123" {
		t.Fatalf("commit=%q", info.Commit)
	}
	if info.RunID != "run-77" {
		t.Fatalf("run_id=%q", info.RunID)
	}
}

func TestFromEnvEmpty(t *testing.T) {
	clearKnownEnv(t)
	if info := FromEnv(); info != nil {
		t.Fatalf("expected nil run info, got %+v", *info)
	}
}

func clearKnownEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"CI",
		"CI_PROVIDER",
		"CI_SYSTEM",
		"CI_PROJECT_PATH",
		"CI_COMMIT_REF_NAME",
		"CI_COMMIT_SHA",
		"CI_PIPELINE_SOURCE",
		"CI_JOB_NAME",
		"CI_PIPELINE_ID",
		"CI_PIPELINE_IID",
		"CI_JOB_URL",
		"GITLAB_CI",
		"GITLAB_USER_LOGIN",
		"BUILDKITE",
		"JENKINS_URL",
		"BUILD_REPOSITORY_NAME",
		"BUILD_SOURCEVERSION",
		"BUILD_DEFINITIONNAME",
		"BUILD_BUILDID",
		"BUILD_BUILDNUMBER",
		"BUILD_BUILDURI",
		"BUILD_URL",
		"BUILD_ID",
		"BUILD_NUMBER",
		"JOB_NAME",
		"BRANCH_NAME",
		"GIT_BRANCH",
		"GIT_COMMIT",
		"SYSTEM_PULLREQUEST_PULLREQUESTNUMBER",
		"PR_NUMBER",
		"BUILD_REQUESTEDFOR",
		"GITHUB_ACTIONS",
		"GITHUB_SERVER_URL",
		"GITHUB_REPOSITORY",
		"GITHUB_REF",
		"GITHUB_REF_NAME",
		"GITHUB_HEAD_REF",
		"GITHUB_SHA",
		"GITHUB_WORKFLOW",
		"GITHUB_JOB",
		"GITHUB_RUN_ID",
		"GITHUB_RUN_NUMBER",
		"GITHUB_EVENT_NAME",
		"GITHUB_ACTOR",
		"GITHUB_PR_NUMBER",
		"SHIRO_CI",
		"SHIRO_CI_PROVIDER",
		"SHIRO_CI_REPOSITORY",
		"SHIRO_CI_BRANCH",
		"SHIRO_CI_COMMIT",
		"SHIRO_CI_WORKFLOW",
		"SHIRO_CI_JOB",
		"SHIRO_CI_RUN_ID",
		"SHIRO_CI_RUN_NUMBER",
		"SHIRO_CI_EVENT",
		"SHIRO_CI_PULL_REQUEST",
		"SHIRO_CI_ACTOR",
		"SHIRO_CI_BUILD_URL",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
}
