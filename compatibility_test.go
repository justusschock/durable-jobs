package jobs_test

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestModuleCompatibilityPolicy(t *testing.T) {
	contents, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	module := string(contents)
	if !regexp.MustCompile(`(?m)^go 1\.25$`).MatchString(module) {
		t.Error("go.mod must declare Go 1.25")
	}
	if strings.Contains(module, "\ntoolchain ") || strings.Contains(module, "\nreplace ") {
		t.Error("go.mod must not contain toolchain or replace directives")
	}
	for _, dependency := range []string{
		"github.com/justusschock/go-distributed-coordination v0.2.0",
		"github.com/justusschock/go-messaging v0.2.0",
		"github.com/justusschock/go-reconciliation v0.2.0",
		"go.etcd.io/bbolt v1.3.11",
	} {
		if !strings.Contains(module, dependency) {
			t.Errorf("go.mod is missing %q", dependency)
		}
	}
	privateVersion := regexp.MustCompile(`github\.com/justusschock/go-[^\s]+\s+(v[^\s]+)`)
	for _, match := range privateVersion.FindAllStringSubmatch(module, -1) {
		if match[1] != "v0.2.0" {
			t.Errorf("private dependency %q is not pinned to v0.2.0", match[0])
		}
	}
}

func TestPrivateModuleGraphPolicy(t *testing.T) {
	command := exec.Command("go", "list", "-m", "all")
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list -m all: %v", err)
	}

	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || !strings.HasPrefix(fields[0], "github.com/justusschock/go-") {
			continue
		}
		if fields[1] != "v0.2.0" {
			t.Errorf("resolved private module %s has version %s, want v0.2.0", fields[0], fields[1])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestCICompatibilityPolicy(t *testing.T) {
	contents, err := os.ReadFile(".github/workflows/ci.yml")
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	workflow := string(contents)

	permissions := yamlIndentedBlock(workflow, "permissions:")
	if strings.TrimSpace(permissions) != "contents: read" {
		t.Errorf("permissions must contain only contents: read, got:\n%s", permissions)
	}

	job := yamlBlockContainingAll(yamlIndentedBlock(workflow, "jobs:"), 2, []string{
		"'1.25.x'", "'1.26.x'",
	})
	if job == "" {
		t.Fatal("no CI job contains the complete supported Go matrix")
	}
	versions := regexp.MustCompile(`(?m)^\s*-\s*'(1\.[0-9]+\.x)'\s*$`).FindAllStringSubmatch(yamlIndentedBlock(job, "go-version:"), -1)
	wantVersions := []string{"1.25.x", "1.26.x"}
	if len(versions) != len(wantVersions) {
		t.Fatalf("Go matrix has %d versions, want %d", len(versions), len(wantVersions))
	}
	for index, want := range wantVersions {
		if versions[index][1] != want {
			t.Errorf("Go matrix version %d = %q, want %q", index, versions[index][1], want)
		}
	}

	requireWorkflowText(t, job,
		"runs-on: ubuntu-latest",
		"fail-fast: false",
		"uses: actions/checkout@v6",
		"uses: actions/setup-go@v6",
		"cache: false",
		"GOPRIVATE: github.com/justusschock/*",
		"GONOSUMDB: github.com/justusschock/*",
		"go mod verify",
		"go list -m all",
		"private modules must resolve at v0.2.0",
		"go mod tidy",
		"git diff --exit-code -- go.mod go.sum",
		"go vet ./...",
		"go test -race -count=1 ./...",
	)
	if strings.Contains(job, "go mod tidy -diff") {
		t.Error("CI must keep the explicit tidy diff check")
	}
	if !regexp.MustCompile(`(?m)^\s+timeout-minutes:\s*[1-9][0-9]*\s*$`).MatchString(job) {
		t.Error("CI job must have a finite positive timeout")
	}

	postgres := yamlBlockContainingAll(yamlIndentedBlock(workflow, "jobs:"), 2, []string{"image: postgres:16"})
	if postgres == "" {
		t.Fatal("no CI job runs the PostgreSQL backend against a postgres:16 service")
	}
	requireWorkflowText(t, postgres,
		"runs-on: ubuntu-latest",
		"uses: actions/checkout@v6",
		"uses: actions/setup-go@v6",
		"cache: false",
		"GOPRIVATE: github.com/justusschock/*",
		"GONOSUMDB: github.com/justusschock/*",
		"DURABLE_JOBS_POSTGRES_DSN: postgres://",
		"working-directory: backend/postgres",
		"go mod verify",
		"go mod tidy",
		"git diff --exit-code -- go.mod go.sum",
		"go vet ./...",
		"go test -race -count=1",
	)
	if !regexp.MustCompile(`(?m)^\s+timeout-minutes:\s*[1-9][0-9]*\s*$`).MatchString(postgres) {
		t.Error("PostgreSQL CI job must have a finite positive timeout")
	}

	secretReference := "${{ secrets.GO_MODULES_TOKEN }}"
	if strings.Count(workflow, secretReference) != 2 {
		t.Errorf("workflow must reference GO_MODULES_TOKEN once per job, got %d", strings.Count(workflow, secretReference))
	}
	if regexp.MustCompile(`https?://[^\n]*(?:secrets\.GO_MODULES_TOKEN|\$\{\{\s*secrets\.GO_MODULES_TOKEN\s*\}\})`).MatchString(workflow) {
		t.Error("GO_MODULES_TOKEN must not be interpolated into a URL")
	}
	for _, credentialJob := range []string{job, postgres} {
		requireCredentialHandling(t, credentialJob, secretReference)
	}
	requireWorkflowText(t, yamlIndentedBlock(workflow, "on:"), "push:", "pull_request:")
}

func TestREADMEReleaseDocumentation(t *testing.T) {
	contents, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(contents)
	for _, stale := range []string{"Initial repository scaffold", "public API has not been implemented"} {
		if strings.Contains(readme, stale) {
			t.Errorf("README contains stale status %q", stale)
		}
	}
	for _, required := range []string{
		"Go 1.25",
		"go get github.com/justusschock/go-durable-jobs@v0.7.0",
		"ExampleDurableClient",
		"ExampleScheduler",
	} {
		if !strings.Contains(readme, required) {
			t.Errorf("README is missing %q", required)
		}
	}
}

func requireCredentialHandling(t *testing.T, job, secretReference string) {
	t.Helper()
	if strings.Count(job, secretReference) != 1 {
		t.Errorf("CI job must reference GO_MODULES_TOKEN exactly once, got %d", strings.Count(job, secretReference))
	}
	credentialStep := yamlStepContaining(job, "GO_MODULES_TOKEN: "+secretReference)
	if credentialStep == "" {
		t.Fatal("GO_MODULES_TOKEN must be supplied through a step environment")
	}
	requireWorkflowText(t, credentialStep,
		"env:",
		"GO_MODULES_TOKEN: "+secretReference,
		`if [ -z "$GO_MODULES_TOKEN" ]; then`,
		`credentials_file="$RUNNER_TEMP/git-credentials"`,
		"umask 077",
		`"$GO_MODULES_TOKEN" > "$credentials_file"`,
		`chmod 0600 "$credentials_file"`,
		`git config --global credential.helper "store --file=$credentials_file"`,
	)
	cleanup := yamlStepContaining(job, "if: always()")
	if cleanup == "" {
		t.Fatal("credential cleanup must run with if: always()")
	}
	requireWorkflowText(t, cleanup, "if: always()", `rm -f "$RUNNER_TEMP/git-credentials"`)
}

func requireWorkflowText(t *testing.T, contents string, requirements ...string) {
	t.Helper()
	for _, requirement := range requirements {
		if !strings.Contains(contents, requirement) {
			t.Errorf("CI workflow is missing %q", requirement)
		}
	}
}

func yamlIndentedBlock(contents, key string) string {
	lines := strings.Split(contents, "\n")
	for index, line := range lines {
		if strings.TrimSpace(line) != key {
			continue
		}
		indent := leadingSpaces(line)
		end := index + 1
		for end < len(lines) {
			if strings.TrimSpace(lines[end]) != "" && leadingSpaces(lines[end]) <= indent {
				break
			}
			end++
		}
		return strings.Join(lines[index+1:end], "\n")
	}
	return ""
}

func yamlBlockContainingAll(contents string, indent int, requirements []string) string {
	lines := strings.Split(contents, "\n")
	for index, line := range lines {
		if leadingSpaces(line) != indent || !strings.HasSuffix(strings.TrimSpace(line), ":") {
			continue
		}
		end := index + 1
		for end < len(lines) {
			if strings.TrimSpace(lines[end]) != "" && leadingSpaces(lines[end]) <= indent {
				break
			}
			end++
		}
		block := strings.Join(lines[index:end], "\n")
		matches := true
		for _, requirement := range requirements {
			matches = matches && strings.Contains(block, requirement)
		}
		if matches {
			return block
		}
	}
	return ""
}

func yamlStepContaining(contents, requirement string) string {
	lines := strings.Split(contents, "\n")
	for index, line := range lines {
		if !strings.Contains(line, requirement) {
			continue
		}
		start := index
		for start >= 0 && !strings.HasPrefix(strings.TrimSpace(lines[start]), "- ") {
			start--
		}
		if start < 0 {
			return ""
		}
		indent := leadingSpaces(lines[start])
		end := start + 1
		for end < len(lines) {
			if leadingSpaces(lines[end]) == indent && strings.HasPrefix(strings.TrimSpace(lines[end]), "- ") {
				break
			}
			end++
		}
		return strings.Join(lines[start:end], "\n")
	}
	return ""
}

func leadingSpaces(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}
