package ui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vornik.io/vornik/internal/persistence"
)

// fakeRetrievalAudit / fakeIngestAudit are in-memory test doubles
// that honour the same filter axes the postgres impls do, so the
// handler's filter-rendering branch is exercised end-to-end without
// a real DB.

type fakeRetrievalAudit struct {
	rows       []*persistence.MemoryRetrievalAudit
	listErr    error
	lastFilter persistence.MemoryRetrievalAuditFilter
}

func (f *fakeRetrievalAudit) Record(_ context.Context, _ *persistence.MemoryRetrievalAudit) error {
	return nil
}
func (f *fakeRetrievalAudit) FeedbackStats(_ context.Context, _ string, _ time.Time) (*persistence.MemoryFeedbackStats, error) {
	return &persistence.MemoryFeedbackStats{}, nil
}
func (f *fakeRetrievalAudit) UnretrievedChunkIDs(_ context.Context, _ string, _ time.Time, _ int) ([]string, error) {
	return nil, nil
}
func (f *fakeRetrievalAudit) List(_ context.Context, filter persistence.MemoryRetrievalAuditFilter) ([]*persistence.MemoryRetrievalAudit, error) {
	f.lastFilter = filter
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := []*persistence.MemoryRetrievalAudit{}
	for _, e := range f.rows {
		if filter.ProjectID != "" && e.ProjectID != filter.ProjectID {
			continue
		}
		if filter.ActorKind != "" {
			if e.ActorKind == nil || *e.ActorKind != filter.ActorKind {
				continue
			}
		}
		if filter.RepoScope != "" {
			if e.RepoScope == nil || *e.RepoScope != filter.RepoScope {
				continue
			}
		}
		if !filter.Since.IsZero() && e.RetrievedAt.Before(filter.Since) {
			continue
		}
		out = append(out, e)
		if filter.PageSize > 0 && len(out) >= filter.PageSize {
			break
		}
	}
	return out, nil
}

func (f *fakeRetrievalAudit) AggregateByActor(_ context.Context, projectID string, since, until time.Time, limit int) ([]persistence.MemoryActorUsage, error) {
	counts := map[[2]string]int{}
	var order [][2]string
	for _, e := range f.rows {
		if projectID != "" && e.ProjectID != projectID {
			continue
		}
		if !since.IsZero() && e.RetrievedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !e.RetrievedAt.Before(until) {
			continue
		}
		key := [2]string{strPtrVal(e.ActorKind), strPtrVal(e.ActorID)}
		if counts[key] == 0 {
			order = append(order, key)
		}
		counts[key]++
	}
	out := make([]persistence.MemoryActorUsage, 0, len(order))
	for _, key := range order {
		out = append(out, persistence.MemoryActorUsage{ActorKind: key[0], ActorID: key[1], CallCount: counts[key]})
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func strPtrVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type fakeIngestAudit struct {
	rows       []*persistence.MemoryIngestAudit
	listErr    error
	lastFilter persistence.MemoryIngestAuditFilter
}

func (f *fakeIngestAudit) Record(_ context.Context, _ *persistence.MemoryIngestAudit) error {
	return nil
}
func (f *fakeIngestAudit) ListByProject(_ context.Context, _ string, _ int) ([]*persistence.MemoryIngestAudit, error) {
	return nil, nil
}
func (f *fakeIngestAudit) List(_ context.Context, filter persistence.MemoryIngestAuditFilter) ([]*persistence.MemoryIngestAudit, error) {
	f.lastFilter = filter
	if f.listErr != nil {
		return nil, f.listErr
	}
	out := []*persistence.MemoryIngestAudit{}
	for _, e := range f.rows {
		if filter.ProjectID != "" && e.ProjectID != filter.ProjectID {
			continue
		}
		if filter.ActorKind != "" {
			if e.ActorKind == nil || *e.ActorKind != filter.ActorKind {
				continue
			}
		}
		if filter.RepoScope != "" {
			if e.RepoScope == nil || *e.RepoScope != filter.RepoScope {
				continue
			}
		}
		if filter.Decision != "" && e.Decision != filter.Decision {
			continue
		}
		if !filter.Since.IsZero() && e.IngestedAt.Before(filter.Since) {
			continue
		}
		out = append(out, e)
		if filter.PageSize > 0 && len(out) >= filter.PageSize {
			break
		}
	}
	return out, nil
}

func (f *fakeIngestAudit) AggregateByActor(_ context.Context, projectID string, since, until time.Time, limit int) ([]persistence.MemoryActorUsage, error) {
	counts := map[[2]string]int{}
	chunks := map[[2]string]int64{}
	var order [][2]string
	for _, e := range f.rows {
		if projectID != "" && e.ProjectID != projectID {
			continue
		}
		if !since.IsZero() && e.IngestedAt.Before(since) {
			continue
		}
		if !until.IsZero() && !e.IngestedAt.Before(until) {
			continue
		}
		key := [2]string{strPtrVal(e.ActorKind), strPtrVal(e.ActorID)}
		if counts[key] == 0 {
			order = append(order, key)
		}
		counts[key]++
		chunks[key] += int64(e.ChunksAdmitted)
	}
	out := make([]persistence.MemoryActorUsage, 0, len(order))
	for _, key := range order {
		out = append(out, persistence.MemoryActorUsage{
			ActorKind: key[0], ActorID: key[1],
			CallCount: counts[key], ChunksAdmitted: chunks[key],
		})
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// TestAdminMemoryAudit_NoReposRendersEmptyState — the page must
// render even when neither repo is wired, so a single-tenant local
// deployment without the SaaS-tier audit machinery still gets a
// usable admin surface (just with "not wired" banners).
func TestAdminMemoryAudit_NoReposRendersEmptyState(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "Memory Audit")
	assert.Contains(t, body, "Retrieval-audit repository not wired")
}

// TestAdminMemoryAudit_RetrievalTabRendersRows — default tab is
// retrieval; rows render with their actor_kind + repo_scope visible
// so operators can spot which surface drove each search.
func TestAdminMemoryAudit_RetrievalTabRendersRows(t *testing.T) {
	repo := &fakeRetrievalAudit{
		rows: []*persistence.MemoryRetrievalAudit{
			{
				ID: "r1", ProjectID: "p1", Query: "find me X",
				ActorKind: stringPtr("companion:claude-code"),
				ActorID:   stringPtr("akey_1"),
				RepoScope: stringPtr("github.com/owner/repo"),
				ChunkIDs:  []string{"c1", "c2"},
			},
		},
	}
	srv := NewServer(WithMemoryRetrievalAuditRepository(repo))
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "find me X")
	assert.Contains(t, body, "companion:claude-code")
	assert.Contains(t, body, "github.com/owner/repo")
	// PageSize defaulted to adminLimitOptions[1] (50) via adminClampLimit.
	assert.Greater(t, repo.lastFilter.PageSize, 0)
}

// TestAdminMemoryAudit_IngestTab — ?tab=ingest flips the active
// panel; the ingest list runs with the supplied filter axes.
func TestAdminMemoryAudit_IngestTab(t *testing.T) {
	repo := &fakeIngestAudit{
		rows: []*persistence.MemoryIngestAudit{
			{
				ID: "ming_1", ProjectID: "p1",
				ActorKind:    stringPtr("companion:claude-code"),
				SourceName:   "decision-foo-20260528",
				Decision:     "admitted",
				ContentBytes: 1234,
				RepoScope:    stringPtr("github.com/owner/repo"),
			},
		},
	}
	srv := NewServer(WithMemoryIngestAuditRepository(repo))
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit?tab=ingest", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "decision-foo-20260528")
	assert.Contains(t, body, "admitted")
}

// TestAdminMemoryAudit_ActorKindFilterReachesRepo — the actor_kind
// axis is the headline B-16 feature ("show me every companion call
// today"). Pin that the URL param survives into the repo filter.
func TestAdminMemoryAudit_ActorKindFilterReachesRepo(t *testing.T) {
	repo := &fakeRetrievalAudit{}
	srv := NewServer(WithMemoryRetrievalAuditRepository(repo))
	req := httptest.NewRequest(http.MethodGet,
		"/admin/memory-audit?actor_kind=companion:claude-code&project=p1", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "companion:claude-code", repo.lastFilter.ActorKind)
	assert.Equal(t, "p1", repo.lastFilter.ProjectID)
}

// TestAdminMemoryAudit_RepoScopeFilterReachesRepo — same shape for
// the repo_scope axis. Pairs with the repo-scope arc (B-1..B-6):
// operators can pull "every search against the VORNIK scope today".
func TestAdminMemoryAudit_RepoScopeFilterReachesRepo(t *testing.T) {
	repo := &fakeRetrievalAudit{}
	srv := NewServer(WithMemoryRetrievalAuditRepository(repo))
	req := httptest.NewRequest(http.MethodGet,
		"/admin/memory-audit?repo_scope=github.com/grinco/vornik", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "github.com/grinco/vornik", repo.lastFilter.RepoScope)
}

// TestAdminMemoryAudit_SinceAcceptsYMD — operators rarely type
// RFC3339 by hand; the YYYY-MM-DD shortcut is more useful and
// matches what chat-audit accepts.
func TestAdminMemoryAudit_SinceAcceptsYMD(t *testing.T) {
	repo := &fakeRetrievalAudit{}
	srv := NewServer(WithMemoryRetrievalAuditRepository(repo))
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit?since=2026-05-28", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.False(t, repo.lastFilter.Since.IsZero())
}

// TestAdminMemoryAudit_ListErrorStillRenders — a repo error must
// not 500 the page; the error message renders inline so the
// operator sees what happened without losing the rest of the UI.
func TestAdminMemoryAudit_ListErrorStillRenders(t *testing.T) {
	repo := &fakeRetrievalAudit{listErr: errors.New("kaboom")}
	srv := NewServer(WithMemoryRetrievalAuditRepository(repo))
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "kaboom")
}

// TestAdminMemoryAudit_RoutedFromAdminRouter — the admin router
// dispatches /admin/memory-audit to the new handler. Confirms the
// case landed in the switch.
func TestAdminMemoryAudit_RoutedFromAdminRouter(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit", nil)
	rec := httptest.NewRecorder()
	srv.adminRouter(rec, withAdminUI(req))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Memory Audit")
	// Sanity-check the tab-strip rendered both tabs.
	body := rec.Body.String()
	assert.True(t, strings.Contains(body, "Retrieval") && strings.Contains(body, "Ingest"))
}

// TestAdminMemoryAudit_ByKeyTabMergesIngestAndRetrieval — the
// headline usage-by-key feature: one actor with both recall and
// remember activity must render as a single merged row (not two),
// with its key identity resolved and a non-companion actor kept
// distinct rather than folded into "Unattributed".
func TestAdminMemoryAudit_ByKeyTabMergesIngestAndRetrieval(t *testing.T) {
	retrieval := &fakeRetrievalAudit{
		rows: []*persistence.MemoryRetrievalAudit{
			{ID: "r1", ProjectID: "p1", Query: "q1", ActorKind: stringPtr("companion:claude-code"), ActorID: stringPtr("akey_1")},
			{ID: "r2", ProjectID: "p1", Query: "q2", ActorKind: stringPtr("agent"), ActorID: stringPtr("kg_extractor")},
		},
	}
	ingest := &fakeIngestAudit{
		rows: []*persistence.MemoryIngestAudit{
			{ID: "ming_1", ProjectID: "p1", ActorKind: stringPtr("companion:claude-code"), ActorID: stringPtr("akey_1"),
				SourceName: "note-1", Decision: "admitted", ChunksAdmitted: 3},
		},
	}
	srv := NewServer(WithMemoryRetrievalAuditRepository(retrieval), WithMemoryIngestAuditRepository(ingest))
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit?tab=by-key", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "companion:claude-code")
	assert.Contains(t, body, "agent")
	// akey_1's row shows both a recall (1) and a remember (1) count —
	// proof the two rollups merged into one row instead of two.
	assert.Contains(t, body, "kg_extractor")
}

// TestAdminMemoryAudit_ByKeyTabNotWiredWhenNeitherRepoPresent —
// same "degrade, don't 500" contract as the other two tabs.
func TestAdminMemoryAudit_ByKeyTabNotWiredWhenNeitherRepoPresent(t *testing.T) {
	srv := NewServer()
	req := httptest.NewRequest(http.MethodGet, "/admin/memory-audit?tab=by-key", nil)
	rec := httptest.NewRecorder()
	srv.AdminMemoryAudit(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Neither audit repository is wired")
}
