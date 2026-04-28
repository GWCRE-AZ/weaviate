//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package inverted

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	invnested "github.com/weaviate/weaviate/adapters/repos/db/inverted/nested"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/filters"
	filnested "github.com/weaviate/weaviate/entities/filters/nested"
	"github.com/weaviate/weaviate/entities/schema"
)

// nestedInfo groups fields that are only relevant for nested (object/object[])
// property filters. The zero value represents a non-nested node.
//
// isNested and isCorrelated are mutually exclusive — a node is either a leaf
// or a group, never both:
//
// When isNested is set (leaf node):
//   - prop on the parent propValuePair is the top-level property name
//   - value is the bare encoded value (without prefix)
//   - relPath is the dot-notation path relative to prop
//     (e.g. "city" or "owner.firstname")
//   - the result bitmap contains positions that are stripped to docIDs
//
// When isCorrelated is set (AND group node):
//   - prop on the parent propValuePair is the root property name
//   - all children require position-aware same-element resolution
//   - childrenFromTokenization marks a compound AND from multi-token text;
//     its children are tokens that must share the same leaf position
type nestedInfo struct {
	isNested                 bool
	relPath                  string
	isCorrelated             bool
	childrenFromTokenization bool
	arrayIndices             arrayIndices
}

// arrayIndices holds positional constraints from arr[N] filter syntax.
// Each entry restricts matching positions to the specified array element.
// Multiple entries support multi-level indexing (e.g. cars[1].tags[2]).
type arrayIndices []filnested.ArrayIndex

// groupKey returns a string that uniquely identifies the set of arr[N] constraints.
// Two conditions with the same groupKey are safe to combine in a correlated AND
// (same-element semantics); different keys require independent resolution.
func (a arrayIndices) groupKey() string {
	if len(a) == 0 {
		return ""
	}
	var key strings.Builder
	for _, ai := range a {
		fmt.Fprintf(&key, "%s[%d]", ai.RelPath, ai.Index)
	}
	return key.String()
}

// fetchNestedDocIDs resolves a value filter on a nested property, returning
// docID-only results. It fetches raw positions, applies any arr[N] index
// constraints, then strips position bits to extract plain docIDs.
func (pv *propValuePair) fetchNestedDocIDs(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	positions, err := pv.fetchNestedPositions(ctx, s, limit)
	if err != nil {
		return nil, err
	}
	defer positions.release()

	dbm := &docBitmap{isDenyList: positions.isDenyList}
	dbm.docIDs, dbm.release = s.nestedBitmapOps.MaskRootLeaf(positions.docIDs)
	return dbm, nil
}

// fetchNestedIsNull resolves an IsNull filter for a nested property by reading
// the existence bitmap from the metadata bucket. relPath="" checks root-level
// existence (e.g. "addresses IsNull"); a non-empty relPath checks sub-property
// existence (e.g. "addresses.city IsNull").
//
// IsNull=false (property exists) → allowlist of matching docIDs.
// IsNull=true  (property absent) → denylist (complement) of matching docIDs.
func (pv *propValuePair) fetchNestedIsNull(s *Searcher) (*docBitmap, error) {
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, errors.Errorf("nested IsNull: meta bucket for %q not found — is it indexed?", pv.prop)
	}

	positionsExists, release, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, fmt.Errorf("nested IsNull: read exists key for %q: %w", pv.prop, err)
	}
	dbmExists, err := pv.restrictByNestedIdx(s, &docBitmap{docIDs: positionsExists, release: release})
	if err != nil {
		return nil, err // restrictByNestedIdx released the bitmap on error
	}
	defer dbmExists.release()

	// pv.value is a little-endian bool: 0x01 = true (IsNull — property absent → denylist).
	dbm := &docBitmap{isDenyList: len(pv.value) > 0 && pv.value[0] == 0x01}
	dbm.docIDs, dbm.release = s.nestedBitmapOps.MaskRootLeaf(dbmExists.docIDs)
	return dbm, nil
}

// fetchNestedPositions fetches the raw position bitmap for a nested value
// filter and applies any arr[N] index constraints. Positions are not stripped
// to docIDs — callers that need docIDs use fetchNestedDocIDs instead; the
// correlated resolution path (resolveNestedCorrelated) uses this directly.
func (pv *propValuePair) fetchNestedPositions(ctx context.Context, s *Searcher, limit int) (*docBitmap, error) {
	raw, err := pv.readFromBucket(ctx, s, limit)
	if err != nil {
		return nil, err
	}
	return pv.restrictByNestedIdx(s, raw)
}

// restrictByNestedIdx restricts a position bitmap to the specific array
// elements recorded in pv.nested.arrayIndices. For each constraint it reads
// IdxKey(relPath, index) from the nested metadata bucket and ANDs it into
// the accumulator using mergeBitmapsAndOrWithDenyList, which selects the more
// efficient accumulator (smaller for AND) and releases the discarded buffer.
// Returns the (potentially swapped) accumulator. On error the current
// accumulator is returned unchanged — the caller is responsible for releasing it.
// No-op when arrayIndices is empty.
func (pv *propValuePair) restrictByNestedIdx(s *Searcher, positions *docBitmap) (*docBitmap, error) {
	if len(pv.nested.arrayIndices) == 0 {
		return positions, nil
	}
	// Release the current accumulator on any non-success exit (error or panic).
	// The closure reads positions at call time, so it always releases the latest
	// accumulator even after mergeBitmapsAndOrWithDenyList swaps it.
	succeeded := false
	defer func() {
		if !succeeded {
			positions.release()
		}
	}()

	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, fmt.Errorf("nested [N] filter: meta bucket for %q not found — is it indexed?", pv.prop)
	}
	var keyBuf [invnested.IdxKeySize]byte
	for _, ai := range pv.nested.arrayIndices {
		positionsIdx, release, err := metaBucket.RoaringSetGet(invnested.IdxKeyToBuf(ai.RelPath, ai.Index, keyBuf[:]))
		if err != nil {
			return nil, fmt.Errorf("nested [N] filter: read idx key for %q[%d]: %w", ai.RelPath, ai.Index, err)
		}
		dbmIdx := &docBitmap{docIDs: positionsIdx, release: release}
		positions = mergeBitmapsAndOrWithDenyList(positions, dbmIdx, filters.OperatorAnd)
	}
	succeeded = true
	return positions, nil
}

// fetchNestedExistsPositions reads the raw position bitmap for a nested property's
// _exists entry from the meta bucket. Unlike fetchNestedIsNull it does NOT apply
// MaskRootLeaf — the caller receives element-level positions suitable for use in
// the correlated resolver (either as an include or an exclude/AndNot bitmap).
// If the filter carries arr[N] constraints (e.g. "cars[1].make IS NULL"),
// restrictByNestedIdx is applied so that only the specified element's positions
// are returned.
func (pv *propValuePair) fetchNestedExistsPositions(s *Searcher) (*sroar.Bitmap, func(), error) {
	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, nil, fmt.Errorf("nested IsNull: meta bucket for %q not found — is it indexed?", pv.prop)
	}
	positions, release, err := metaBucket.RoaringSetGet(invnested.ExistsKey(pv.nested.relPath))
	if err != nil {
		return nil, nil, fmt.Errorf("nested IsNull: read exists key for %q: %w", pv.prop, err)
	}
	if len(pv.nested.arrayIndices) == 0 {
		return positions, release, nil
	}
	// Restrict to the specific array element(s) indicated by arr[N] constraints.
	dbm := &docBitmap{docIDs: positions, release: release}
	restricted, err := pv.restrictByNestedIdx(s, dbm)
	if err != nil {
		return nil, nil, err
	}
	return restricted.docIDs, restricted.release, nil
}

// positionBitmaps groups pre-fetched raw position bitmaps for a single nested path,
// split by origin so the executor can apply the correct combining strategy:
//   - tokens: from childrenFromTokenization compound ANDs (multi-token text);
//     combined with AndAll — all tokens must share the same leaf position.
//   - independent: from direct leaf conditions (e.g. scalar array values);
//     combined with MaskLeafAndAll when there are multiple — values may be at
//     different leaf positions within the same parent element.
type positionBitmaps struct {
	tokens      []*sroar.Bitmap
	independent []*sroar.Bitmap
}

// resolveNestedCorrelated resolves one prop group using position-aware
// correlation. It builds a resolutionPlan for the group's paths, pre-computes
// raw position bitmaps, and executes the plan to enforce same-element semantics.
//
// When children have different arr[N] constraints they are partitioned by their
// arrayIndices key. The AND strategy depends on whether any partition is unconstrained:
//
//   - All groups explicitly constrained (e.g. nestedArray[0].x AND nestedArray[1].y):
//     the user is intentionally querying different elements → docID-level AND.
//
//   - Any group unconstrained ("" key) mixed with constrained (e.g.
//     garages[1].make="honda" AND garages.city="berlin"):
//     the unconstrained condition must match the SAME root element as the
//     constrained one → root+docID AND.
func (pv *propValuePair) resolveNestedCorrelated(ctx context.Context, s *Searcher) (*docBitmap, error) {
	groups := groupChildrenByArrayIndicesKey(pv.children)
	switch len(groups) {
	case 0:
		return nil, fmt.Errorf("nested correlated AND: no condition groups for %q", pv.prop)
	case 1:
		return pv.resolveNestedCorrelatedGroup(ctx, s, groups[0])
	}

	// Determine whether any group is unconstrained (arr[N] key == "").
	hasUnconstrained := false
	for _, g := range groups {
		if groupKeyOf(g) == "" {
			hasUnconstrained = true
			break
		}
	}

	if !hasUnconstrained {
		// All groups carry explicit different indices — the user is querying
		// different elements independently. Resolve each group to plain docIDs
		// and AND at docID level (original multi-group behavior).
		dbm, err := pv.resolveNestedCorrelatedGroup(ctx, s, groups[0])
		if err != nil {
			return nil, err
		}
		succeeded := false
		defer func() {
			if !succeeded {
				dbm.release()
			}
		}()
		for _, group := range groups[1:] {
			groupDbm, err := pv.resolveNestedCorrelatedGroup(ctx, s, group)
			if err != nil {
				return nil, err
			}
			dbm = mergeBitmapsAndOrWithDenyList(dbm, groupDbm, filters.OperatorAnd)
		}
		succeeded = true
		return dbm, nil
	}

	// Has an unconstrained group: all conditions must be satisfied by the SAME
	// root element. Resolve each group to root+docID positions and AND them so
	// that conditions from different root elements cannot combine.
	var releases []func()
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range releases {
				rel()
			}
		}
	}()

	masked, rel, err := pv.resolveNestedCorrelatedGroupMasked(ctx, s, groups[0])
	if err != nil {
		return nil, err
	}
	releases = append(releases, rel)

	for _, group := range groups[1:] {
		groupMasked, groupRel, err := pv.resolveNestedCorrelatedGroupMasked(ctx, s, group)
		if err != nil {
			return nil, err
		}
		releases = append(releases, groupRel)
		masked.AndConc(groupMasked, concurrency.SROAR_MERGE)
	}

	docIDs, docRelease := s.nestedBitmapOps.MaskRootLeaf(masked)
	succeeded = true
	for _, rel := range releases {
		rel()
	}
	return &docBitmap{docIDs: docIDs, release: docRelease}, nil
}

// resolveNestedCorrelatedGroup resolves a single set of children using
// position-aware same-element correlation. All children must have compatible
// arrayIndices (same arr[N] constraints at every level).
func (pv *propValuePair) resolveNestedCorrelatedGroup(ctx context.Context, s *Searcher, children []*propValuePair) (*docBitmap, error) {
	// Build positionsByPath: fetch raw position bitmaps per pvp and route into
	// the correct bucket slot based on origin (token vs independent).
	positionsByPath := make(map[string]*positionBitmaps, len(children))
	paths := make([]string, 0, len(children))

	// releases collects the pool-buffer release functions from every fetched
	// position bitmap. They are deferred until after execute() returns, at
	// which point the executor has finished reading all position bitmaps and
	// it is safe to return the backing buffers to the pool.
	var releases []func()
	defer func() {
		for _, rel := range releases {
			rel()
		}
	}()

	// excludePositions collects raw position bitmaps for IsNull=true conditions.
	// They are passed to the plan executor which applies AndNot after leaf-masking
	// the main result, removing elements where the property IS present.
	var excludePositions []*sroar.Bitmap

	fetchAndRoute := func(leaf *propValuePair, isToken bool) error {
		if leaf.operator == filters.OperatorIsNull {
			// IsNull conditions are resolved via the _exists metadata entry, not the
			// filterable value bucket. Both cases read the same bitmap of positions
			// where the property IS present.
			positions, release, err := leaf.fetchNestedExistsPositions(s)
			if err != nil {
				return err
			}
			releases = append(releases, release)
			isDenyList := len(leaf.value) > 0 && leaf.value[0] == 0x01
			if isDenyList {
				// IsNull=true (property absent): exclude elements where property IS present.
				excludePositions = append(excludePositions, positions)
			} else {
				// IsNull=false (property exists): include as regular independent condition.
				path := leaf.nested.relPath
				if _, exists := positionsByPath[path]; !exists {
					positionsByPath[path] = &positionBitmaps{}
					paths = append(paths, path)
				}
				positionsByPath[path].independent = append(positionsByPath[path].independent, positions)
			}
			return nil
		}

		dbm, err := leaf.fetchNestedPositions(ctx, s, 0)
		if err != nil {
			return err
		}
		releases = append(releases, dbm.release)
		path := leaf.nested.relPath
		if _, exists := positionsByPath[path]; !exists {
			positionsByPath[path] = &positionBitmaps{}
			paths = append(paths, path)
		}
		if isToken {
			positionsByPath[path].tokens = append(positionsByPath[path].tokens, dbm.docIDs)
		} else {
			positionsByPath[path].independent = append(positionsByPath[path].independent, dbm.docIDs)
		}
		return nil
	}

	for _, child := range children {
		if child.nested.isNested {
			// When pv itself is a tokenization compound AND, its children are tokens
			// of the same value and must share the same leaf position → route as tokens.
			if err := fetchAndRoute(child, pv.nested.childrenFromTokenization); err != nil {
				return nil, fmt.Errorf("nested correlated AND: fetch bitmap for %q: %w", child.nested.relPath, err)
			}
		} else {
			// Tokenization compound AND child: grandchildren are tokens of the same value.
			for _, gc := range child.children {
				if err := fetchAndRoute(gc, true); err != nil {
					return nil, fmt.Errorf("nested correlated AND: fetch bitmap for %q: %w", gc.nested.relPath, err)
				}
			}
		}
	}

	// Find the root property schema and meta bucket — always needed for plan
	// building and potential rootAnchor fetch.
	rootProp, err := schema.GetPropertyByName(pv.Class, pv.prop)
	if err != nil {
		return nil, fmt.Errorf("nested correlated AND: root property %q not found in schema: %w", pv.prop, err)
	}

	metaBucket := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
	if metaBucket == nil {
		return nil, fmt.Errorf("nested correlated AND: meta bucket for %q not found — is it indexed?", pv.prop)
	}

	counts := make(conditionCounts, len(positionsByPath))
	for path, positions := range positionsByPath {
		counts[path] = [2]int{len(positions.tokens), len(positions.independent)}
	}

	// The plan builder is the single authority on execution strategy. When all
	// conditions are IsNull=true (paths is empty), it sets useRootAnchor=true so
	// the executor knows to use the root _exists bitmap as the element universe.
	plan, err := newExecutionPlanBuilder(rootProp.NestedProperties).build(paths, counts)
	if err != nil {
		return nil, fmt.Errorf("nested correlated AND: build plan for %q: %w", pv.prop, err)
	}

	// Fetch rootAnchor only when the plan requires it — i.e. when the plan
	// builder decided all conditions are IsNull=true.
	var rootAnchor *sroar.Bitmap
	if plan.useRootAnchor {
		anchor, anchorRelease, err := pv.fetchRootAnchor(s, metaBucket, children)
		if err != nil {
			return nil, fmt.Errorf("nested correlated AND: fetch root anchor for %q: %w", pv.prop, err)
		}
		releases = append(releases, anchorRelease)
		rootAnchor = anchor
	}

	// TODO aliszka:nested_filtering concurrency.SROAR_MERGE is a fixed budget;
	// consider deriving it from the request context or shard-level config.
	docIDs, release, err := newPlanExecutor(plan, positionsByPath, metaBucket, s.nestedBitmapOps, concurrency.SROAR_MERGE, rootAnchor, excludePositions...).execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("nested correlated AND: execute for %q: %w", pv.prop, err)
	}

	return &docBitmap{docIDs: docIDs, release: release}, nil
}

// resolveNestedCorrelatedGroupMasked is the same as resolveNestedCorrelatedGroup
// but returns root+docID positions (leaf bits zeroed, root bits preserved) rather
// than plain docIDs. Used by resolveNestedCorrelated when multiple groups must be
// AND'd at root+docID level to enforce same-root-element semantics.
func (pv *propValuePair) resolveNestedCorrelatedGroupMasked(ctx context.Context, s *Searcher, children []*propValuePair) (*sroar.Bitmap, func(), error) {
	// Build the same executor as resolveNestedCorrelatedGroup — share the
	// setup by delegating to the plan executor's executeMasked method.
	// We replicate the positionsByPath / plan building inline to avoid a
	// large refactor; the logic is identical to resolveNestedCorrelatedGroup.
	positionsByPath := make(map[string]*positionBitmaps, len(children))
	paths := make([]string, 0, len(children))

	var excludePositions []*sroar.Bitmap
	var releases []func()
	succeeded := false
	defer func() {
		if !succeeded {
			for _, rel := range releases {
				rel()
			}
		}
	}()

	fetchAndRoute := func(leaf *propValuePair, isToken bool) error {
		if leaf.operator == filters.OperatorIsNull {
			positions, release, err := leaf.fetchNestedExistsPositions(s)
			if err != nil {
				return err
			}
			releases = append(releases, release)
			isDenyList := len(leaf.value) > 0 && leaf.value[0] == 0x01
			if isDenyList {
				excludePositions = append(excludePositions, positions)
			} else {
				path := leaf.nested.relPath
				if _, exists := positionsByPath[path]; !exists {
					positionsByPath[path] = &positionBitmaps{}
					paths = append(paths, path)
				}
				positionsByPath[path].independent = append(positionsByPath[path].independent, positions)
			}
			return nil
		}

		dbm, err := leaf.fetchNestedPositions(ctx, s, 0)
		if err != nil {
			return err
		}
		releases = append(releases, dbm.release)
		path := leaf.nested.relPath
		if _, exists := positionsByPath[path]; !exists {
			positionsByPath[path] = &positionBitmaps{}
			paths = append(paths, path)
		}
		if isToken {
			positionsByPath[path].tokens = append(positionsByPath[path].tokens, dbm.docIDs)
		} else {
			positionsByPath[path].independent = append(positionsByPath[path].independent, dbm.docIDs)
		}
		return nil
	}

	for _, child := range children {
		if child.nested.isNested {
			if err := fetchAndRoute(child, pv.nested.childrenFromTokenization); err != nil {
				return nil, nil, fmt.Errorf("nested correlated AND: fetch bitmap for %q: %w", child.nested.relPath, err)
			}
		} else {
			for _, gc := range child.children {
				if err := fetchAndRoute(gc, true); err != nil {
					return nil, nil, fmt.Errorf("nested correlated AND: fetch bitmap for %q: %w", gc.nested.relPath, err)
				}
			}
		}
	}

	var rootAnchor *sroar.Bitmap
	var plan executionPlan
	if len(positionsByPath) == 0 && len(excludePositions) > 0 {
		mb := s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
		if mb == nil {
			return nil, nil, fmt.Errorf("nested correlated AND: meta bucket for %q not found — is it indexed?", pv.prop)
		}
		anchor, anchorRelease, err := pv.fetchRootAnchor(s, mb, children)
		if err != nil {
			return nil, nil, fmt.Errorf("nested correlated AND: fetch root anchor for %q: %w", pv.prop, err)
		}
		releases = append(releases, anchorRelease)
		rootAnchor = anchor
		plan.useRootAnchor = true
	}
	var metaBucket *lsmkv.Bucket
	if len(positionsByPath) > 0 {
		rootProp, err := schema.GetPropertyByName(pv.Class, pv.prop)
		if err != nil {
			return nil, nil, fmt.Errorf("nested correlated AND: root property %q not found in schema: %w", pv.prop, err)
		}
		metaBucket = s.store.Bucket(helpers.BucketNestedMetaFromPropNameLSM(pv.prop))
		if metaBucket == nil {
			return nil, nil, fmt.Errorf("nested correlated AND: meta bucket for %q not found — is it indexed?", pv.prop)
		}
		counts := make(conditionCounts, len(positionsByPath))
		for path, positions := range positionsByPath {
			counts[path] = [2]int{len(positions.tokens), len(positions.independent)}
		}
		var err2 error
		plan, err2 = newExecutionPlanBuilder(rootProp.NestedProperties).build(paths, counts)
		if err2 != nil {
			return nil, nil, fmt.Errorf("nested correlated AND: build plan for %q: %w", pv.prop, err2)
		}
	}

	masked, maskedRelease, err := newPlanExecutor(plan, positionsByPath, metaBucket, s.nestedBitmapOps, concurrency.SROAR_MERGE, rootAnchor, excludePositions...).executeMasked(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("nested correlated AND: executeMasked for %q: %w", pv.prop, err)
	}

	succeeded = true
	allReleases := append(releases, maskedRelease)
	return masked, func() {
		for _, rel := range allReleases {
			rel()
		}
	}, nil
}

// fetchRootAnchor returns the bitmap of element positions used as the starting
// universe when all conditions in a correlated group are IsNull=true (no positive
// anchor). It reads _exists."" from the meta bucket and, if any child carries
// arr[N] constraints, restricts it to only the specified element positions via
// restrictByNestedIdx — so that "garages[1].make IS NULL" starts from garage[1]
// positions only, not all garages.
func (pv *propValuePair) fetchRootAnchor(s *Searcher, metaBucket *lsmkv.Bucket, children []*propValuePair) (*sroar.Bitmap, func(), error) {
	rootPositions, rootRelease, err := metaBucket.RoaringSetGet(invnested.ExistsKey(""))
	if err != nil {
		return nil, nil, err
	}

	// Find arr[N] constraints from any leaf child (all children in the group share the same key).
	var arrayIndices []filnested.ArrayIndex
	for _, child := range children {
		if child.nested.isNested && len(child.nested.arrayIndices) > 0 {
			arrayIndices = child.nested.arrayIndices
			break
		}
		for _, gc := range child.children {
			if len(gc.nested.arrayIndices) > 0 {
				arrayIndices = gc.nested.arrayIndices
				break
			}
		}
		if len(arrayIndices) > 0 {
			break
		}
	}

	if len(arrayIndices) == 0 {
		return rootPositions, rootRelease, nil
	}

	// Apply arr[N] restriction using a temporary propValuePair that carries only
	// the array indices — restrictByNestedIdx reads _idx.{relPath}[N] entries.
	tempPvp := &propValuePair{
		prop:   pv.prop,
		nested: nestedInfo{arrayIndices: arrayIndices},
		Class:  pv.Class,
	}
	dbm := &docBitmap{docIDs: rootPositions, release: rootRelease}
	restricted, err := tempPvp.restrictByNestedIdx(s, dbm)
	if err != nil {
		return nil, nil, err
	}
	return restricted.docIDs, restricted.release, nil
}

// groupKeyOf returns the arr[N] constraint key for a group produced by
// groupChildrenByArrayIndicesKey. It mirrors the key-computation logic used
// inside that function so callers can inspect group keys without re-partitioning.
func groupKeyOf(group []*propValuePair) string {
	if len(group) == 0 {
		return ""
	}
	child := group[0]
	if child.nested.isNested {
		return child.nested.arrayIndices.groupKey()
	}
	if len(child.children) > 0 {
		return child.children[0].nested.arrayIndices.groupKey()
	}
	return ""
}

// groupChildrenByArrayIndicesKey partitions the children of a correlated AND
// node by their arr[N] constraint key. Children with the same key can be
// combined with same-element semantics; children with different keys must be
// resolved independently. A single group is returned when all children are
// compatible (the common case when no arr[N] constraints are used).
func groupChildrenByArrayIndicesKey(children []*propValuePair) [][]*propValuePair {
	seen := map[string]int{} // key → index into groups
	var groups [][]*propValuePair

	for _, child := range children {
		var key string
		if child.nested.isNested {
			key = child.nested.arrayIndices.groupKey()
		} else if len(child.children) > 0 {
			// Tokenization compound AND: all grandchildren share the same arrayIndices.
			key = child.children[0].nested.arrayIndices.groupKey()
		}
		if idx, ok := seen[key]; ok {
			groups[idx] = append(groups[idx], child)
		} else {
			seen[key] = len(groups)
			groups = append(groups, []*propValuePair{child})
		}
	}

	return groups
}
