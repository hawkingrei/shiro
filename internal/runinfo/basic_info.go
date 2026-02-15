package runinfo

import (
	"os"
	"regexp"
	"strings"
)

var githubPullRefPattern = regexp.MustCompile(`^refs/pull/([0-9]+)/`)

// BasicInfo captures CI/run metadata for logs and case reports.
type BasicInfo struct {
	CI          bool   `json:"ci,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Repository  string `json:"repository,omitempty"`
	Branch      string `json:"branch,omitempty"`
	Commit      string `json:"commit,omitempty"`
	Workflow    string `json:"workflow,omitempty"`
	Job         string `json:"job,omitempty"`
	RunID       string `json:"run_id,omitempty"`
	RunNumber   string `json:"run_number,omitempty"`
	Event       string `json:"event,omitempty"`
	PullRequest string `json:"pull_request,omitempty"`
	Actor       string `json:"actor,omitempty"`
	BuildURL    string `json:"build_url,omitempty"`
}

// FromEnv builds run metadata from environment variables.
// Explicit SHIRO_CI_* values take precedence over provider defaults.
func FromEnv() *BasicInfo {
	info := detectBase()
	applyShiroOverrides(&info)
	normalize(&info)
	if info.IsZero() {
		return nil
	}
	return &info
}

// IsZero reports whether all fields are empty.
func (b BasicInfo) IsZero() bool {
	if !b.CI &&
		b.Provider == "" &&
		b.Repository == "" &&
		b.Branch == "" &&
		b.Commit == "" &&
		b.Workflow == "" &&
		b.Job == "" &&
		b.RunID == "" &&
		b.RunNumber == "" &&
		b.Event == "" &&
		b.PullRequest == "" &&
		b.Actor == "" &&
		b.BuildURL == "" {
		return true
	}
	return false
}

func detectBase() BasicInfo {
	info := BasicInfo{}

	if isTruthy(env("GITHUB_ACTIONS")) {
		info.CI = true
		info.Provider = "github_actions"
		info.Repository = env("GITHUB_REPOSITORY")
		info.Branch = envFirst("GITHUB_HEAD_REF", "GITHUB_REF_NAME")
		info.Commit = env("GITHUB_SHA")
		info.Workflow = env("GITHUB_WORKFLOW")
		info.Job = env("GITHUB_JOB")
		info.RunID = env("GITHUB_RUN_ID")
		info.RunNumber = env("GITHUB_RUN_NUMBER")
		info.Event = env("GITHUB_EVENT_NAME")
		info.Actor = env("GITHUB_ACTOR")
		info.PullRequest = env("GITHUB_PR_NUMBER")
		if info.PullRequest == "" {
			info.PullRequest = githubPullRequestFromRef(env("GITHUB_REF"))
		}
		serverURL := env("GITHUB_SERVER_URL")
		if serverURL == "" {
			serverURL = "https://github.com"
		}
		if info.Repository != "" && info.RunID != "" {
			info.BuildURL = strings.TrimRight(serverURL, "/") + "/" + info.Repository + "/actions/runs/" + info.RunID
		}
	}

	if isTruthy(env("GITLAB_CI")) {
		info.CI = true
		if info.Provider == "" {
			info.Provider = "gitlab_ci"
		}
	}
	if isTruthy(env("BUILDKITE")) {
		info.CI = true
		if info.Provider == "" {
			info.Provider = "buildkite"
		}
	}
	if env("JENKINS_URL") != "" {
		info.CI = true
		if info.Provider == "" {
			info.Provider = "jenkins"
		}
	}
	if isTruthy(env("CI")) {
		info.CI = true
	}

	setIfEmpty(&info.Provider, strings.ToLower(envFirst("CI_PROVIDER", "CI_SYSTEM")))
	setIfEmpty(&info.Repository, envFirst("CI_PROJECT_PATH", "BUILD_REPOSITORY_NAME"))
	setIfEmpty(&info.Branch, envFirst("CI_COMMIT_REF_NAME", "BRANCH_NAME", "GIT_BRANCH"))
	setIfEmpty(&info.Commit, envFirst("CI_COMMIT_SHA", "GIT_COMMIT", "BUILD_SOURCEVERSION"))
	setIfEmpty(&info.Workflow, envFirst("CI_PIPELINE_SOURCE", "BUILD_DEFINITIONNAME"))
	setIfEmpty(&info.Job, envFirst("CI_JOB_NAME", "JOB_NAME"))
	setIfEmpty(&info.RunID, envFirst("CI_PIPELINE_ID", "BUILD_BUILDID", "BUILD_ID"))
	setIfEmpty(&info.RunNumber, envFirst("CI_PIPELINE_IID", "BUILD_BUILDNUMBER", "BUILD_NUMBER"))
	setIfEmpty(&info.Event, envFirst("CI_PIPELINE_SOURCE"))
	setIfEmpty(&info.PullRequest, envFirst("SYSTEM_PULLREQUEST_PULLREQUESTNUMBER", "PR_NUMBER"))
	setIfEmpty(&info.Actor, envFirst("GITLAB_USER_LOGIN", "BUILD_REQUESTEDFOR"))
	setIfEmpty(&info.BuildURL, envFirst("CI_JOB_URL", "BUILD_URL", "BUILD_BUILDURI"))

	return info
}

func applyShiroOverrides(info *BasicInfo) {
	if info == nil {
		return
	}
	explicit := false
	explicitCI := false
	if v, ok := lookupTrimmed("SHIRO_CI"); ok && v != "" {
		info.CI = isTruthy(v)
		explicit = true
		explicitCI = true
	}
	explicit = setFromEnv(&info.Provider, "SHIRO_CI_PROVIDER") || explicit
	explicit = setFromEnv(&info.Repository, "SHIRO_CI_REPOSITORY") || explicit
	explicit = setFromEnv(&info.Branch, "SHIRO_CI_BRANCH") || explicit
	explicit = setFromEnv(&info.Commit, "SHIRO_CI_COMMIT") || explicit
	explicit = setFromEnv(&info.Workflow, "SHIRO_CI_WORKFLOW") || explicit
	explicit = setFromEnv(&info.Job, "SHIRO_CI_JOB") || explicit
	explicit = setFromEnv(&info.RunID, "SHIRO_CI_RUN_ID") || explicit
	explicit = setFromEnv(&info.RunNumber, "SHIRO_CI_RUN_NUMBER") || explicit
	explicit = setFromEnv(&info.Event, "SHIRO_CI_EVENT") || explicit
	explicit = setFromEnv(&info.PullRequest, "SHIRO_CI_PULL_REQUEST") || explicit
	explicit = setFromEnv(&info.Actor, "SHIRO_CI_ACTOR") || explicit
	explicit = setFromEnv(&info.BuildURL, "SHIRO_CI_BUILD_URL") || explicit
	if explicit && !explicitCI && !info.CI {
		info.CI = true
	}
}

func normalize(info *BasicInfo) {
	if info == nil {
		return
	}
	info.Provider = strings.TrimSpace(strings.ToLower(info.Provider))
	info.Repository = strings.TrimSpace(info.Repository)
	info.Branch = normalizeBranch(info.Branch)
	info.Commit = strings.TrimSpace(info.Commit)
	info.Workflow = strings.TrimSpace(info.Workflow)
	info.Job = strings.TrimSpace(info.Job)
	info.RunID = strings.TrimSpace(info.RunID)
	info.RunNumber = strings.TrimSpace(info.RunNumber)
	info.Event = strings.TrimSpace(info.Event)
	info.PullRequest = strings.TrimSpace(info.PullRequest)
	info.Actor = strings.TrimSpace(info.Actor)
	info.BuildURL = strings.TrimSpace(info.BuildURL)
	if info.PullRequest == "" {
		info.PullRequest = githubPullRequestFromRef(env("GITHUB_REF"))
	}
	if !info.CI && (info.Provider != "" || info.Repository != "" || info.RunID != "" || info.Commit != "") && !shiroCIExplicitFalse() {
		info.CI = true
	}
	if info.CI && info.Provider == "" {
		info.Provider = "generic"
	}
}

func shiroCIExplicitFalse() bool {
	v, ok := lookupTrimmed("SHIRO_CI")
	return ok && v != "" && !isTruthy(v)
}

func normalizeBranch(branch string) string {
	branch = strings.TrimSpace(branch)
	branch = strings.TrimPrefix(branch, "refs/heads/")
	branch = strings.TrimPrefix(branch, "origin/")
	return branch
}

func githubPullRequestFromRef(ref string) string {
	ref = strings.TrimSpace(ref)
	m := githubPullRefPattern.FindStringSubmatch(ref)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

func env(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

func envFirst(keys ...string) string {
	for _, key := range keys {
		if value := env(key); value != "" {
			return value
		}
	}
	return ""
}

func setIfEmpty(dst *string, value string) {
	if dst == nil || *dst != "" || value == "" {
		return
	}
	*dst = value
}

func lookupTrimmed(key string) (string, bool) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}
	return strings.TrimSpace(value), true
}

func setFromEnv(dst *string, key string) bool {
	if dst == nil {
		return false
	}
	value, ok := lookupTrimmed(key)
	if !ok || value == "" {
		return false
	}
	*dst = value
	return true
}

func isTruthy(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
