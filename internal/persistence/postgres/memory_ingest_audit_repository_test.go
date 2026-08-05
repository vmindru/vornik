package postgres

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMemoryIngestAuditRepo(t *testing.T) (*MemoryIngestAuditRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return NewMemoryIngestAuditRepository(db), mock, func() { _ = db.Close() }
}

// TestMemoryIngestAudit_AggregateByActor pins the (actor_kind, actor_id)
// grouping and the LEFT JOIN against api_keys that resolves a
// companion actor's identity — see persistence.MemoryActorUsage for why
// only "companion:*" actors resolve to a key.
func TestMemoryIngestAudit_AggregateByActor(t *testing.T) {
	repo, mock, cleanup := newMemoryIngestAuditRepo(t)
	defer cleanup()

	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)

	rows := sqlmock.NewRows([]string{
		"actor_kind", "actor_id", "key_name", "key_prefix", "session_label", "client_kind",
		"call_count", "chunks_admitted",
	}).
		AddRow("companion:claude-code", "akey-1", "vadim/laptop", "vk_abcd", "vadim/laptop", "claude-code", int64(5), int64(9)).
		AddRow("agent", "kg_extractor", "", "", "", "", int64(2), int64(2))

	mock.ExpectQuery(regexp.QuoteMeta(
		"GROUP BY m.actor_kind, m.actor_id, k.name, k.key_prefix, k.session_label, k.client_kind\n\t\tORDER BY call_count DESC")).
		WithArgs("p-1", since, until, 25).
		WillReturnRows(rows)

	out, err := repo.AggregateByActor(context.Background(), "p-1", since, until, 25)
	if err != nil {
		t.Fatalf("AggregateByActor: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(out), out)
	}
	if out[0].ActorKind != "companion:claude-code" || out[0].KeyName != "vadim/laptop" || out[0].CallCount != 5 || out[0].ChunksAdmitted != 9 {
		t.Errorf("row 0 roundtrip: %+v", out[0])
	}
	if out[1].ActorKind != "agent" || out[1].KeyName != "" {
		t.Errorf("agent row should not resolve a key identity: %+v", out[1])
	}
}
