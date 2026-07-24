package memory

import "testing"

func TestScopeClassificationValid(t *testing.T) {
	valid := ScopeClassification{ChunkID: "c1", Decision: ScopeDecisionMove, Confidence: 0.91, Reason: "matches", ContentHash: "h"}
	if !valid.Valid() {
		t.Fatal("valid classification rejected")
	}
	valid.Confidence = 1.1
	if valid.Valid() {
		t.Fatal("invalid confidence accepted")
	}
}
