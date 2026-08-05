package sqlite_test

import (
	"context"
	"testing"
	"time"

	"vornik.io/vornik/internal/persistence"
	"vornik.io/vornik/internal/persistence/sqlite"
)

// TestMemoryIngestAudit_AggregateByActor seeds one companion actor (with
// a resolvable api_keys row) and one agent actor (whose actor_id is a
// role name, not a key) and pins that only the companion row resolves
// an identity — same convention as AggregateByAPIKey.
func TestMemoryIngestAudit_AggregateByActor(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := sqlite.NewMemoryIngestAuditRepository(db.DB)

	if _, err := db.ExecContext(ctx, `INSERT INTO api_keys
		(id, project_id, name, key_hash, key_prefix, created_at, session_label, client_kind)
		VALUES ('akey-1', 'p', 'vadim/laptop', 'h_akey1', 'vk_abcd', datetime('now'), 'vadim/laptop', 'claude-code')`); err != nil {
		t.Fatalf("insert api_keys fixture: %v", err)
	}

	companionKind := "companion:claude-code"
	companionID := "akey-1"
	agentKind := "agent"
	agentID := "kg_extractor"
	rows := []persistence.MemoryIngestAudit{
		{ID: "ming-1", ProjectID: "p", ActorKind: &companionKind, ActorID: &companionID, SourceName: "note-1", ContentHash: "h1", Decision: persistence.MemoryIngestAuditAdmitted, ChunksAdmitted: 2},
		{ID: "ming-2", ProjectID: "p", ActorKind: &companionKind, ActorID: &companionID, SourceName: "note-2", ContentHash: "h2", Decision: persistence.MemoryIngestAuditAdmitted, ChunksAdmitted: 3},
		{ID: "ming-3", ProjectID: "p", ActorKind: &agentKind, ActorID: &agentID, SourceName: "note-3", ContentHash: "h3", Decision: persistence.MemoryIngestAuditAdmitted, ChunksAdmitted: 1},
	}
	for i := range rows {
		if err := repo.Record(ctx, &rows[i]); err != nil {
			t.Fatalf("Record %s: %v", rows[i].ID, err)
		}
	}

	out, err := repo.AggregateByActor(ctx, "p", time.Time{}, time.Time{}, 10)
	if err != nil {
		t.Fatalf("AggregateByActor: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 actor buckets, got %d: %+v", len(out), out)
	}
	// Call-count sorted descending — the companion actor (2 calls) comes first.
	if out[0].ActorKind != "companion:claude-code" || out[0].KeyName != "vadim/laptop" || out[0].KeyPrefix != "vk_abcd" {
		t.Errorf("row 0 = %+v, want resolved to akey-1/vadim/laptop/vk_abcd", out[0])
	}
	if out[0].CallCount != 2 || out[0].ChunksAdmitted != 5 {
		t.Errorf("row 0 call/chunk counts = %d/%d, want 2/5", out[0].CallCount, out[0].ChunksAdmitted)
	}
	if out[1].ActorKind != "agent" || out[1].KeyName != "" {
		t.Errorf("agent row should not resolve a key identity, got %+v", out[1])
	}
	if out[1].CallCount != 1 || out[1].ChunksAdmitted != 1 {
		t.Errorf("row 1 call/chunk counts = %d/%d, want 1/1", out[1].CallCount, out[1].ChunksAdmitted)
	}
}

// TestMemoryRetrievalAudit_AggregateByActor mirrors the ingest-side
// test; ChunksAdmitted always stays zero (ingest-only field).
func TestMemoryRetrievalAudit_AggregateByActor(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := sqlite.NewMemoryRetrievalAuditRepository(db.DB)

	if _, err := db.ExecContext(ctx, `INSERT INTO api_keys
		(id, project_id, name, key_hash, key_prefix, created_at, session_label, client_kind)
		VALUES ('akey-2', 'p', 'vadim/desktop', 'h_akey2', 'vk_efgh', datetime('now'), 'vadim/desktop', 'codex')`); err != nil {
		t.Fatalf("insert api_keys fixture: %v", err)
	}

	companionKind := "companion:codex"
	companionID := "akey-2"
	rows := []persistence.MemoryRetrievalAudit{
		{ID: "retr-1", ProjectID: "p", Query: "q1", ActorKind: &companionKind, ActorID: &companionID},
		{ID: "retr-2", ProjectID: "p", Query: "q2", ActorKind: &companionKind, ActorID: &companionID},
		{ID: "retr-3", ProjectID: "p", Query: "q3"}, // no actor_kind/actor_id at all — legacy row
	}
	for i := range rows {
		if err := repo.Record(ctx, &rows[i]); err != nil {
			t.Fatalf("Record %s: %v", rows[i].ID, err)
		}
	}

	out, err := repo.AggregateByActor(ctx, "p", time.Time{}, time.Time{}, 10)
	if err != nil {
		t.Fatalf("AggregateByActor: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 actor buckets, got %d: %+v", len(out), out)
	}
	if out[0].ActorKind != "companion:codex" || out[0].KeyName != "vadim/desktop" || out[0].CallCount != 2 {
		t.Errorf("row 0 = %+v, want resolved companion:codex/vadim/desktop with 2 calls", out[0])
	}
	if out[0].ChunksAdmitted != 0 {
		t.Errorf("retrieval rollup should never populate ChunksAdmitted, got %d", out[0].ChunksAdmitted)
	}
	if out[1].ActorKind != "" || out[1].ActorID != "" {
		t.Errorf("legacy no-identity row should bucket under empty actor_kind, got %+v", out[1])
	}
	if out[1].CallCount != 1 {
		t.Errorf("row 1 call count = %d, want 1", out[1].CallCount)
	}
}
