package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	rpctypes "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func TestFormatKeepAliveCloseDiagnostic(t *testing.T) {
	message := formatKeepAliveCloseDiagnostic(42, nil, 7, 5, 1250*time.Millisecond)
	for _, expected := range []string{
		"lease_id=42",
		"reason=channel_closed_without_cancel",
		"responses=7",
		"last_ttl_seconds=5",
		"last_response_age_ms=1250",
	} {
		if !strings.Contains(message, expected) {
			t.Fatalf("diagnostic %q does not contain %q", message, expected)
		}
	}

	message = formatKeepAliveCloseDiagnostic(43, context.Canceled, 0, 0, -1)
	if !strings.Contains(message, "reason=context_cancelled") || !strings.Contains(message, "lease_id=43") {
		t.Fatalf("unexpected cancellation diagnostic: %q", message)
	}
}

func TestClassifyKeepAliveTTLProbe(t *testing.T) {
	tests := []struct {
		name string
		ttl  int64
		err  error
		want string
	}{
		{name: "active", ttl: 3, want: "lease_alive"},
		{name: "zero ttl", want: "lease_not_found"},
		{name: "lease absent", err: rpctypes.ErrLeaseNotFound, want: "lease_not_found"},
		{name: "transport error", err: errors.New("unavailable"), want: "probe_error"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := classifyKeepAliveTTLProbe(test.ttl, test.err); got != test.want {
				t.Fatalf("classifyKeepAliveTTLProbe(%d, %v) = %q, want %q", test.ttl, test.err, got, test.want)
			}
		})
	}
}

func TestNewMaintenanceSessionCancelsStartup(t *testing.T) {
	original := startMaintenanceSession
	defer func() { startMaintenanceSession = original }()

	started := make(chan struct{})
	startMaintenanceSession = func(ctx context.Context, _ *clientv3.Client,
		_ int) (*concurrency.Session, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	result := make(chan error, 1)
	go func() {
		_, err := newMaintenanceSession(nil, 30, 10*time.Millisecond)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup was not attempted")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected startup cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("maintenance session startup did not cancel")
	}
}
