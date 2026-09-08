package userservice

import (
	"bytes"
	"strings"
)

func parseLaunchctlStatus(output []byte) Status {
	for _, line := range bytes.Split(output, []byte{'\n'}) {
		if strings.TrimSpace(string(line)) == "state = running" {
			return StatusActive
		}
	}
	return StatusInactive
}
