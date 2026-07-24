package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"vornik.io/vornik/internal/chat"
)

// ScopeDecision is the only action a scope-classifier report may propose.
// Reports are advisory; retagging remains a separate explicit operation.
type ScopeDecision string

const (
	ScopeDecisionKeep   ScopeDecision = "keep"
	ScopeDecisionReview ScopeDecision = "review"
	ScopeDecisionMove   ScopeDecision = "move"
)

// ScopeClassification is one reviewable proposal for a memory chunk.
// It deliberately contains no mutation fields: callers must pass approved
// chunk IDs to the separate scope-retag operation.
type ScopeClassification struct {
	ChunkID     string        `json:"chunk_id"`
	Decision    ScopeDecision `json:"decision"`
	Confidence  float64       `json:"confidence"`
	Reason      string        `json:"reason"`
	ContentHash string        `json:"content_hash"`
}

func (c ScopeClassification) Valid() bool {
	if c.ChunkID == "" || c.Confidence < 0 || c.Confidence > 1 || c.Reason == "" {
		return false
	}
	return c.Decision == ScopeDecisionKeep || c.Decision == ScopeDecisionReview || c.Decision == ScopeDecisionMove
}

// ScopeClassifier proposes a scope migration decision. It never writes memory.
type ScopeClassifier struct{ Client chat.Provider }

func (c ScopeClassifier) Classify(ctx context.Context, id, hash, description, content string) (ScopeClassification, error) {
	if c.Client == nil {
		return ScopeClassification{}, fmt.Errorf("scope classifier: client not configured")
	}
	resp, err := c.Client.Complete(ctx, []chat.Message{
		{Role: "system", Content: "Classify whether a memory chunk matches a target scope description. Return JSON only: {\"decision\":\"keep|review|move\",\"confidence\":0..1,\"reason\":\"short reason\"}. Never claim certainty when ambiguous."},
		{Role: "user", Content: "Target description:\n" + description + "\n\nChunk:\n" + content},
	})
	if err != nil {
		return ScopeClassification{}, err
	}
	if resp == nil || len(resp.Choices) == 0 {
		return ScopeClassification{}, fmt.Errorf("scope classifier: no choices")
	}
	var out ScopeClassification
	if err := json.Unmarshal([]byte(strings.TrimSpace(resp.Choices[0].Message.Content)), &out); err != nil {
		return ScopeClassification{}, fmt.Errorf("scope classifier: parse response: %w", err)
	}
	out.ChunkID, out.ContentHash = id, hash
	if !out.Valid() {
		return ScopeClassification{}, fmt.Errorf("scope classifier: invalid response")
	}
	return out, nil
}
