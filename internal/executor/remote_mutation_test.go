package executor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/rijuyuezhu/pkudisk-sync/internal/domain"
)

type commandCall struct {
	name string
	args []string
	opts map[string]string
}

type fakeRemoteCommander struct {
	calls  []commandCall
	result any
	err    error
}

func (f *fakeRemoteCommander) Command(_ context.Context, name string, args []string, opts map[string]string) (any, error) {
	copiedArgs := append([]string(nil), args...)
	var copiedOpts map[string]string
	if opts != nil {
		copiedOpts = make(map[string]string, len(opts))
		for k, v := range opts {
			copiedOpts[k] = v
		}
	}
	f.calls = append(f.calls, commandCall{name: name, args: copiedArgs, opts: copiedOpts})
	return f.result, f.err
}

func TestDeleteRemoteFileUsesExactIDAndRevision(t *testing.T) {
	remote := &fakeRemoteCommander{}
	exec := &RootExecutor{remotePKU: remote}
	expected := domain.RemoteExpectation{ID: "doc-file", Rev: "rev-7"}
	if err := exec.DeleteRemoteFile(context.Background(), expected); err != nil {
		t.Fatal(err)
	}
	want := []commandCall{{name: "sync-delete", args: []string{"doc-file"}, opts: map[string]string{"expected-rev": "rev-7"}}}
	if !reflect.DeepEqual(remote.calls, want) {
		t.Fatalf("delete calls = %#v, want %#v", remote.calls, want)
	}
}

func TestDeleteRemoteFilePropagatesGuardFailure(t *testing.T) {
	guardErr := errors.New("sync precondition failed")
	remote := &fakeRemoteCommander{err: guardErr}
	exec := &RootExecutor{remotePKU: remote}
	err := exec.DeleteRemoteFile(context.Background(), domain.RemoteExpectation{ID: "doc-file", Rev: "rev-7"})
	if !errors.Is(err, guardErr) {
		t.Fatalf("DeleteRemoteFile() error = %v, want wrapped %v", err, guardErr)
	}
}

func TestDeleteRemoteDirUsesExactIDWithoutInventingRevisionCAS(t *testing.T) {
	remote := &fakeRemoteCommander{}
	exec := &RootExecutor{remotePKU: remote}
	if err := exec.DeleteRemoteDir(context.Background(), domain.RemoteExpectation{ID: "doc-dir"}); err != nil {
		t.Fatal(err)
	}
	want := []commandCall{{name: "sync-delete-dir", args: []string{"doc-dir"}, opts: nil}}
	if !reflect.DeepEqual(remote.calls, want) {
		t.Fatalf("directory delete calls = %#v, want %#v", remote.calls, want)
	}
}

func TestDeleteRemoteDirRejectsFileRevisionExpectationBeforeBackendCall(t *testing.T) {
	remote := &fakeRemoteCommander{}
	exec := &RootExecutor{remotePKU: remote}
	if err := exec.DeleteRemoteDir(context.Background(), domain.RemoteExpectation{ID: "doc-dir", Rev: "rev-not-valid-for-dir"}); err == nil {
		t.Fatal("DeleteRemoteDir accepted a revision-bearing directory expectation")
	}
	if len(remote.calls) != 0 {
		t.Fatalf("invalid directory delete reached backend: %#v", remote.calls)
	}
}
