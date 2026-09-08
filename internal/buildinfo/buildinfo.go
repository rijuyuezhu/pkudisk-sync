package buildinfo

// These values are overridden with -ldflags for release builds. Keeping safe
// development defaults makes ordinary `go build` and tests self-describing.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)
