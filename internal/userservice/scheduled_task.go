package userservice

import (
	"encoding/csv"
	"strings"
)

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

func scheduledTaskCreateArgs(executable string) []string {
	command := `"` + executable + `" daemon`
	return []string{
		"/create",
		"/tn", scheduledTaskName,
		"/tr", command,
		"/sc", "ONLOGON",
		"/it",
		"/rl", "LIMITED",
		"/f",
	}
}

func parseScheduledTaskStatus(output []byte) Status {
	rows, err := csv.NewReader(strings.NewReader(string(output))).ReadAll()
	if err != nil || len(rows) == 0 || len(rows[0]) < 3 {
		return Status("installed")
	}
	state := strings.TrimSpace(rows[0][2])
	if state == "" {
		return Status("installed")
	}
	return Status(state)
}
