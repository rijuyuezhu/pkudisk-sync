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
	for _, want := range []string{
		"<string>daemon</string>",
		"<string>--service</string>",
		"<key>SuccessfulExit</key>",
		"<false/>",
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("plist missing %q:\n%s", want, plist)
		}
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
