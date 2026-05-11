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

package errors

import (
	"context"

	"github.com/pkg/errors"
)

// ErrShardRecovering: shard data is missing locally and being copied
// from a peer (SELF_RECOVERY). Defense-in-depth for callers that
// bypass the router (which already filters via the replication FSM).
var ErrShardRecovering = errors.New("shard recovering from peer")

func IsShardRecovering(err error) bool {
	return errors.Is(err, ErrShardRecovering)
}

// fromSchemaReloadKey marks a context as originating from
// reloadDBFromSchema (snapshot Restore or post-catchup reload). This
// distinguishes "AddClass for a pre-existing class being re-loaded"
// (a SELF_RECOVERY candidate when the on-disk dir is missing) from
// "AddClass for a brand-new class" (where a missing dir is normal).
type fromSchemaReloadKey struct{}

// WithFromSchemaReload tags the ctx as a snapshot-reload entry point.
func WithFromSchemaReload(ctx context.Context) context.Context {
	return context.WithValue(ctx, fromSchemaReloadKey{}, true)
}

// IsFromSchemaReload reports whether ctx was tagged via WithFromSchemaReload.
func IsFromSchemaReload(ctx context.Context) bool {
	v, _ := ctx.Value(fromSchemaReloadKey{}).(bool)
	return v
}
