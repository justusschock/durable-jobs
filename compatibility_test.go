package jobs_test

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// NFR-JOBS-05: the core module supports Go 1.25 as its floor and carries no
// toolchain or replace directives that would leak into consumers' builds.
func TestModuleCompatibilityPolicy(t *testing.T) {
	contents, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	module := string(contents)
	if !regexp.MustCompile(`(?m)^go 1\.25(\.0)?$`).MatchString(module) {
		t.Error("go.mod must declare Go 1.25 as the minimum version")
	}
	if strings.Contains(module, "\ntoolchain ") || strings.Contains(module, "\nreplace ") {
		t.Error("go.mod must not contain toolchain or replace directives")
	}
}
