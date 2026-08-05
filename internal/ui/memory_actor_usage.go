package ui

import (
	"context"
	"sort"
	"time"

	"vornik.io/vornik/internal/persistence"
)

// MemoryUsageByKeyRow is one row of a per-actor usage rollup, merging
// the ingest and retrieval AggregateByActor results so an actor with
// either kind of activity shows once instead of twice. Answers "who is
// using RAG" — a call-count question the spend dashboard's cost
// attribution structurally can't answer for task-less traffic like the
// kg_extraction background worker (see persistence.MemoryActorUsage).
//
// NoIdentity is true only for the true "no actor_kind at all" bucket
// (legacy pre-migration-72 rows). "agent"/"rest_api"/"ui" actor kinds
// are meaningfully labelled even without a resolved key and must not
// be collapsed into this bucket.
type MemoryUsageByKeyRow struct {
	ActorKind      string
	ActorID        string
	KeyName        string
	KeyPrefix      string
	SessionLabel   string
	ClientKind     string
	RecallCalls    int
	RememberCalls  int
	ChunksAdmitted int64
	NoIdentity     bool
}

// TotalCalls is RecallCalls + RememberCalls — the sort key and the
// headline number templates render.
func (r MemoryUsageByKeyRow) TotalCalls() int { return r.RecallCalls + r.RememberCalls }

// mergeMemoryActorUsage combines an ingest rollup and a retrieval
// rollup into one row per (actor_kind, actor_id), sorted by total
// calls descending. Safe to call with actors from multiple projects
// already concatenated: actor_id is either an api_keys.id (which
// belongs to exactly one project) or a non-key identity (agent role
// name, or empty) — merging those across an all-projects view is the
// same convention the spend page's Unattributed bucket already uses.
func mergeMemoryActorUsage(ingest, retrieval []persistence.MemoryActorUsage) []MemoryUsageByKeyRow {
	type actorKey struct{ kind, id string }
	rows := map[actorKey]*MemoryUsageByKeyRow{}
	var order []actorKey

	upsert := func(u persistence.MemoryActorUsage, isIngest bool) {
		k := actorKey{u.ActorKind, u.ActorID}
		row, ok := rows[k]
		if !ok {
			row = &MemoryUsageByKeyRow{
				ActorKind:    u.ActorKind,
				ActorID:      u.ActorID,
				KeyName:      u.KeyName,
				KeyPrefix:    u.KeyPrefix,
				SessionLabel: u.SessionLabel,
				ClientKind:   u.ClientKind,
				NoIdentity:   u.ActorKind == "",
			}
			rows[k] = row
			order = append(order, k)
		}
		if isIngest {
			row.RememberCalls += u.CallCount
			row.ChunksAdmitted += u.ChunksAdmitted
		} else {
			row.RecallCalls += u.CallCount
		}
	}
	for _, u := range ingest {
		upsert(u, true)
	}
	for _, u := range retrieval {
		upsert(u, false)
	}

	out := make([]MemoryUsageByKeyRow, 0, len(order))
	for _, k := range order {
		out = append(out, *rows[k])
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].TotalCalls() > out[j].TotalCalls() })
	return out
}

// memoryActorUsageForScope loads and merges the ingest/retrieval
// per-actor rollups across the given project scope. iter follows the
// same convention as apiKeySpendForScope: [""] queries globally in one
// call, otherwise one call per project, merged. Missing repos degrade
// to an empty slice rather than an error — callers render a "not
// available" state instead of failing the page.
func (s *Server) memoryActorUsageForScope(ctx context.Context, iter []string, since time.Time) []MemoryUsageByKeyRow {
	var allIngest, allRetrieval []persistence.MemoryActorUsage
	if s.memoryIngestAudit != nil {
		for _, pid := range iter {
			rows, err := s.memoryIngestAudit.AggregateByActor(ctx, pid, since, time.Time{}, 0)
			if err != nil {
				s.logger.Warn().Err(err).Str("project_id", pid).Msg("memory usage-by-key: ingest aggregate failed")
				continue
			}
			allIngest = append(allIngest, rows...)
		}
	}
	if s.memoryRetrievalAudit != nil {
		for _, pid := range iter {
			rows, err := s.memoryRetrievalAudit.AggregateByActor(ctx, pid, since, time.Time{}, 0)
			if err != nil {
				s.logger.Warn().Err(err).Str("project_id", pid).Msg("memory usage-by-key: retrieval aggregate failed")
				continue
			}
			allRetrieval = append(allRetrieval, rows...)
		}
	}
	return mergeMemoryActorUsage(allIngest, allRetrieval)
}
