package syncer

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

// validateSnapshotNamespace verifies that the observed local and remote path
// spellings can coexist in one target platform's local namespace. It never
// rewrites names: an ambiguous or unrepresentable namespace fails closed before
// reconciliation can journal or execute mutations.
func validateSnapshotNamespace(local map[string]domain.LocalFingerprint, remote map[string]domain.RemoteFingerprint, targetOS string) error {
	type observedPath struct {
		side string
		rel  string
	}
	paths := make([]observedPath, 0, len(local)+len(remote))
	for rel := range local {
		paths = append(paths, observedPath{side: "local", rel: rel})
	}
	for rel := range remote {
		paths = append(paths, observedPath{side: "remote", rel: rel})
	}
	sort.Slice(paths, func(i, j int) bool {
		if paths[i].rel != paths[j].rel {
			return paths[i].rel < paths[j].rel
		}
		return paths[i].side < paths[j].side
	})

	type owner struct {
		side string
		rel  string
	}
	owners := make(map[string]owner, len(paths))
	for _, item := range paths {
		key, err := localNamespaceKey(item.rel, targetOS)
		if err != nil {
			return fmt.Errorf("%s path %q is not representable on %s: %w", item.side, item.rel, targetOS, err)
		}
		if previous, ok := owners[key]; ok && previous.rel != item.rel {
			return fmt.Errorf("namespace collision on %s: %s path %q and %s path %q map to the same local path", targetOS, previous.side, previous.rel, item.side, item.rel)
		}
		owners[key] = owner{side: item.side, rel: item.rel}
	}
	return nil
}

func localNamespaceKey(rel, targetOS string) (string, error) {
	if err := domain.ValidateRelPath(rel); err != nil {
		return "", err
	}
	segments := strings.Split(rel, "/")
	keys := make([]string, len(segments))
	for i, segment := range segments {
		if !utf8.ValidString(segment) {
			return "", fmt.Errorf("path component is not valid UTF-8")
		}
		if strings.IndexByte(segment, 0) >= 0 {
			return "", fmt.Errorf("path component contains NUL")
		}
		switch targetOS {
		case "linux":
			keys[i] = segment
		case "windows":
			if err := validateWindowsComponent(segment); err != nil {
				return "", err
			}
			keys[i] = simpleCaseFold(segment)
		case "darwin":
			// v0.1 deliberately uses the conservative namespace of the default
			// case-insensitive, normalization-insensitive macOS filesystem. A
			// case-sensitive APFS volume may represent more names, but accepting
			// them would make the same root unsafe on the common configuration.
			keys[i] = simpleCaseFold(norm.NFC.String(segment))
		default:
			return "", fmt.Errorf("unsupported target platform %q", targetOS)
		}
	}
	return strings.Join(keys, "/"), nil
}

func simpleCaseFold(value string) string {
	// Windows/macOS filesystem comparison is not locale-sensitive. A one-rune
	// Unicode mapping is intentionally used instead of linguistic full folding,
	// which could reject spellings the filesystem itself keeps distinct.
	return strings.Map(unicode.ToLower, value)
}

func validateWindowsComponent(component string) error {
	if component == "" {
		return fmt.Errorf("empty path component")
	}
	if strings.HasSuffix(component, " ") || strings.HasSuffix(component, ".") {
		return fmt.Errorf("path component %q ends in a space or dot", component)
	}
	for _, r := range component {
		if r < 32 || strings.ContainsRune(`<>:"/\\|?*`, r) {
			return fmt.Errorf("path component %q contains a Windows-reserved character", component)
		}
	}

	base := component
	if dot := strings.IndexByte(base, '.'); dot >= 0 {
		base = base[:dot]
	}
	base = strings.TrimRight(base, " .")
	upper := strings.ToUpper(base)
	switch upper {
	case "CON", "PRN", "AUX", "NUL", "CLOCK$", "CONIN$", "CONOUT$":
		return fmt.Errorf("path component %q uses a Windows-reserved device name", component)
	}
	if isWindowsNumberedDevice(upper, "COM") || isWindowsNumberedDevice(upper, "LPT") {
		return fmt.Errorf("path component %q uses a Windows-reserved device name", component)
	}
	return nil
}

func isWindowsNumberedDevice(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(value, prefix)
	if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' {
		return true
	}
	return suffix == "¹" || suffix == "²" || suffix == "³"
}
