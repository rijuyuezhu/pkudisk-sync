package userservice

import (
	"reflect"
	"testing"
)

func TestScheduledTaskCreateArgsUseCurrentInteractiveUserAtLimitedPrivilege(t *testing.T) {
	executable := `C:\Program Files\PKU Disk\pkudisk-sync.exe`
	got := scheduledTaskCreateArgs(executable)
	want := []string{
		"/create",
		"/tn", scheduledTaskName,
		"/tr", `"C:\Program Files\PKU Disk\pkudisk-sync.exe" daemon --service`,
		"/sc", "ONLOGON",
		"/it",
		"/rl", "LIMITED",
		"/f",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled task create args = %#v, want %#v", got, want)
	}
}

func TestScheduledTaskQueryArgsUseHRESULTExitCodes(t *testing.T) {
	got := scheduledTaskQueryArgs()
	want := []string{"/query", "/tn", scheduledTaskName, "/fo", "CSV", "/nh", "/hresult"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled task query args = %#v, want %#v", got, want)
	}
	if !isScheduledTaskNotFoundExitCode(0x80070002) || !isScheduledTaskNotFoundExitCode(0x8004130F) {
		t.Fatal("known task-not-found HRESULT was not recognized")
	}
	if isScheduledTaskNotFoundExitCode(0x80070005) {
		t.Fatal("access denied was misclassified as task-not-found")
	}
}

func TestParseScheduledTaskStatus(t *testing.T) {
	if got := parseScheduledTaskStatus([]byte(`"\\PKUDisk Sync","N/A","Running"` + "\r\n")); got != Status("Running") {
		t.Fatalf("parseScheduledTaskStatus() = %q", got)
	}
	if got := parseScheduledTaskStatus([]byte("not csv enough\r\n")); got != Status("installed") {
		t.Fatalf("fallback status = %q", got)
	}
}
