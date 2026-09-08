package userservice

import (
	"reflect"
	"strings"
	"testing"
)

func TestScheduledTaskInstallArgsUseCurrentInteractiveUserAtLimitedPrivilege(t *testing.T) {
	executable := `C:\Users\O'Brien\PKU Disk\pkudisk-sync.exe`
	got := scheduledTaskInstallArgs(executable)
	wantScript := strings.Join([]string{
		`$ErrorActionPreference = 'Stop'`,
		`$user = [Security.Principal.WindowsIdentity]::GetCurrent().Name`,
		`$action = New-ScheduledTaskAction -Execute 'C:\Users\O''Brien\PKU Disk\pkudisk-sync.exe' -Argument 'daemon --service'`,
		`$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user`,
		`$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited`,
		`$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal`,
		`Register-ScheduledTask -TaskName 'PKUDisk Sync' -InputObject $task -Force | Out-Null`,
	}, `; `)
	want := []string{"-NoProfile", "-NonInteractive", "-Command", wantScript}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled task install args = %#v, want %#v", got, want)
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

func TestScheduledTaskStatusArgsAreLocaleIndependent(t *testing.T) {
	got := scheduledTaskStatusArgs()
	wantScript := strings.Join([]string{
		`$task = Get-ScheduledTask -TaskName 'PKUDisk Sync' -ErrorAction SilentlyContinue`,
		`if ($null -eq $task) { Write-Output 'not-installed'; exit 0 }`,
		`if ($task.State -eq 'Running') { Write-Output 'active' } else { Write-Output 'inactive' }`,
	}, `; `)
	want := []string{"-NoProfile", "-NonInteractive", "-Command", wantScript}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduled task status args = %#v, want %#v", got, want)
	}
}
