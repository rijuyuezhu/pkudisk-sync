package userservice

import "testing"

func TestParseLaunchctlStatus(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   Status
	}{
		{
			name: "running",
			output: `gui/501/io.github.rijuyuezhu.pkudisk-sync = {
	state = running
	pid = 1234
}`,
			want: StatusActive,
		},
		{
			name: "loaded but not running",
			output: `gui/501/io.github.rijuyuezhu.pkudisk-sync = {
	state = waiting
	last exit code = 1
}`,
			want: StatusInactive,
		},
		{
			name: "nonempty output without state",
			output: `gui/501/io.github.rijuyuezhu.pkudisk-sync = {
	path = /Users/test/Library/LaunchAgents/io.github.rijuyuezhu.pkudisk-sync.plist
}`,
			want: StatusInactive,
		},
		{
			name:   "near match",
			output: "previous state = running\n",
			want:   StatusInactive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseLaunchctlStatus([]byte(tt.output)); got != tt.want {
				t.Fatalf("parseLaunchctlStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}
