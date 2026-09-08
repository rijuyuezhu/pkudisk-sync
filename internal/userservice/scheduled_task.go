package userservice

import "strings"

const (
	scheduledTaskName            = "PKUDisk Sync"
	scheduledTaskNotFoundHRESULT = uint32(0x80070002)
	scheduledTaskNotFoundCode    = uint32(0x8004130F)
)

func scheduledTaskQueryArgs() []string {
	return []string{"/query", "/tn", scheduledTaskName, "/fo", "CSV", "/nh", "/hresult"}
}

func isScheduledTaskNotFoundExitCode(code uint32) bool {
	return code == scheduledTaskNotFoundHRESULT || code == scheduledTaskNotFoundCode
}

func scheduledTaskInstallArgs(executable string) []string {
	script := strings.Join([]string{
		`$ErrorActionPreference = 'Stop'`,
		`$user = [Security.Principal.WindowsIdentity]::GetCurrent().Name`,
		`$action = New-ScheduledTaskAction -Execute ` + powerShellSingleQuote(executable) + ` -Argument 'daemon --service'`,
		`$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user`,
		`$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited`,
		`$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal`,
		`Register-ScheduledTask -TaskName ` + powerShellSingleQuote(scheduledTaskName) + ` -InputObject $task -Force | Out-Null`,
	}, `; `)
	return []string{"-NoProfile", "-NonInteractive", "-Command", script}
}

func powerShellSingleQuote(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

func scheduledTaskStatusArgs() []string {
	script := strings.Join([]string{
		`$task = Get-ScheduledTask -TaskName ` + powerShellSingleQuote(scheduledTaskName) + ` -ErrorAction SilentlyContinue`,
		`if ($null -eq $task) { Write-Output 'not-installed'; exit 0 }`,
		`if ($task.State -eq 'Running') { Write-Output 'active' } else { Write-Output 'inactive' }`,
	}, `; `)
	return []string{"-NoProfile", "-NonInteractive", "-Command", script}
}
