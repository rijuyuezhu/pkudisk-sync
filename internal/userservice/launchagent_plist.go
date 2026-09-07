package userservice

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"
)

const launchAgentLabel = "io.github.rijuyuezhu.pkudisk-sync"

func renderLaunchAgentPlist(executable string) (string, error) {
	if executable == "" || strings.ContainsAny(executable, "\x00\r\n") {
		return "", fmt.Errorf("invalid launch agent executable")
	}
	var escaped bytes.Buffer
	if err := xml.EscapeText(&escaped, []byte(executable)); err != nil {
		return "", fmt.Errorf("escape launch agent executable: %w", err)
	}
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>` + launchAgentLabel + `</string>
  <key>ProgramArguments</key>
  <array>
    <string>` + escaped.String() + `</string>
    <string>daemon</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
`, nil
}
