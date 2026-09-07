package reconcile

import "testing"

func TestDecideRecovery(t *testing.T) {
	tests := []struct {
		name        string
		observation RecoveryObservation
		want        RecoveryAction
		wantErr     bool
	}{
		{
			name:        "postcondition already satisfied commits without replay",
			observation: RecoveryObservation{Outcome: RecoveryPostconditionSatisfied},
			want:        RecoveryCommit,
		},
		{
			name:        "definitely not applied with original preconditions retries",
			observation: RecoveryObservation{Outcome: RecoveryNotApplied, PreconditionsHold: true},
			want:        RecoveryRetry,
		},
		{
			name:        "definitely not applied but stale preconditions reconciles",
			observation: RecoveryObservation{Outcome: RecoveryNotApplied, PreconditionsHold: false},
			want:        RecoveryReconcile,
		},
		{
			name:        "ambiguous outcome reconciles instead of replaying",
			observation: RecoveryObservation{Outcome: RecoveryAmbiguous, PreconditionsHold: true},
			want:        RecoveryReconcile,
		},
		{
			name:        "divergence blocks",
			observation: RecoveryObservation{Outcome: RecoveryDiverged},
			want:        RecoveryBlock,
		},
		{
			name:        "invalid outcome errors",
			observation: RecoveryObservation{Outcome: "unknown"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecideRecovery(tt.observation)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("DecideRecovery(%+v) unexpectedly succeeded with %q", tt.observation, got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("DecideRecovery(%+v) = %q, want %q", tt.observation, got, tt.want)
			}
		})
	}
}
