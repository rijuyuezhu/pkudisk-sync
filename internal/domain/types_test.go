package domain

import "testing"

func TestValidateRelPath(t *testing.T) {
	valid := []string{"a", "dir/file.txt", "目录/文件"}
	for _, rel := range valid {
		if err := ValidateRelPath(rel); err != nil {
			t.Errorf("ValidateRelPath(%q) = %v", rel, err)
		}
	}

	invalid := []string{"", ".", "../a", "a/../b", "/absolute", "a\\b", "a//b", "a/./b"}
	for _, rel := range invalid {
		if err := ValidateRelPath(rel); err == nil {
			t.Errorf("ValidateRelPath(%q) unexpectedly succeeded", rel)
		}
	}
}

func TestLocalEquivalentDirectoriesIgnoreDirectoryMtime(t *testing.T) {
	a := LocalFingerprint{Present: true, Kind: KindDir, MtimeNS: 1}
	b := LocalFingerprint{Present: true, Kind: KindDir, MtimeNS: 2}
	if !LocalEquivalent(a, b) {
		t.Fatal("directory mtime must not represent child-content changes")
	}
}

func TestRemoteEquivalentUsesFileRevisionAndDirectoryIdentity(t *testing.T) {
	fileA := RemoteFingerprint{Present: true, Kind: KindFile, ID: "id", Rev: "a", Size: 1}
	fileB := RemoteFingerprint{Present: true, Kind: KindFile, ID: "id", Rev: "b", Size: 1}
	if RemoteEquivalent(fileA, fileB) {
		t.Fatal("different file revisions must be treated as changed")
	}

	dirA := RemoteFingerprint{Present: true, Kind: KindDir, ID: "a"}
	dirB := RemoteFingerprint{Present: true, Kind: KindDir, ID: "b"}
	if RemoteEquivalent(dirA, dirB) {
		t.Fatal("different directory IDs must be treated as changed")
	}
}
