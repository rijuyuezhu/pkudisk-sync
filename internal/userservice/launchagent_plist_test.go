package userservice

import (
	"encoding/xml"
	"strings"
	"testing"
)

func TestRenderLaunchAgentPlistIsWellFormedAndEscapesExecutable(t *testing.T) {
	plist, err := renderLaunchAgentPlist(`/Users/Test & Dev/<sync>/pkudisk-sync`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plist, `/Users/Test &amp; Dev/&lt;sync&gt;/pkudisk-sync`) {
		t.Fatalf("plist did not XML-escape executable:\n%s", plist)
	}
	if !strings.Contains(plist, "<string>daemon</string>") || !strings.Contains(plist, "<true/>") {
		t.Fatalf("plist missing daemon/keepalive fields:\n%s", plist)
	}
	decoder := xml.NewDecoder(strings.NewReader(plist))
	for {
		if _, err := decoder.Token(); err != nil {
			if err.Error() == "EOF" {
				break
			}
			t.Fatalf("plist XML parse: %v", err)
		}
	}
}
