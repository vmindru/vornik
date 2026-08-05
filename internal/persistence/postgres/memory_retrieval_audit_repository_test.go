package postgres

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMemoryRetrievalAuditRepo(t *testing.T) (*MemoryRetrievalAuditRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return NewMemoryRetrievalAuditRepository(db), mock, func() { _ = db.Close() }
}

// TestMemoryRetrievalAudit_AggregateByActor mirrors the ingest-side
// test: same (actor_kind, actor_id) grouping and api_keys join, minus
// the ingest-only chunks_admitted column.
func TestMemoryRetrievalAudit_AggregateByActor(t *testing.T) {
	repo, mock, cleanup := newMemoryRetrievalAuditRepo(t)
	defer cleanup()

	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	rows := sqlmock.NewRows([]string{
		"actor_kind", "actor_id", "key_name", "key_prefix", "session_label", "client_kind", "call_count",
	}).
		AddRow("companion:codex", "akey-2", "vadim/desktop", "vk_efgh", "vadim/desktop", "codex", int64(3)).
		AddRow("", "", "", "", "", "", int64(1))

	mock.ExpectQuery(regexp.QuoteMeta(
		"GROUP BY m.actor_kind, m.actor_id, k.name, k.key_prefix, k.session_label, k.client_kind\n\t\tORDER BY call_count DESC")).
		WithArgs("p-1", since, 10).
		WillReturnRows(rows)

	out, err := repo.AggregateByActor(context.Background(), "p-1", since, time.Time{}, 10)
	if err != nil {
		t.Fatalf("AggregateByActor: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d: %+v", len(out), out)
	}
	if out[0].ActorKind != "companion:codex" || out[0].KeyName != "vadim/desktop" || out[0].CallCount != 3 {
		t.Errorf("row 0 roundtrip: %+v", out[0])
	}
	if out[1].ActorKind != "" || out[1].ActorID != "" {
		t.Errorf("row 1 should be the true no-identity bucket: %+v", out[1])
	}
}
