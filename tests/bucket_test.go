package nkv_test

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blizzard/nkv.go"

	"github.com/matryer/is"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCreateBucketNames(t *testing.T) {
	nc := testConnection(t)

	valid := []string{
		"A",
		"bucket_123",
		"bucket-with-dashes",
	}
	for _, name := range valid {
		t.Run("valid/"+name, func(t *testing.T) {
			is := is.New(t)
			kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: name})
			is.NoErr(err)                     // valid bucket name should be accepted
			is.Equal(kv.Name(), name)         // bucket should retain the valid name
			is.Equal(kv.Stream(), "KV_"+name) // stream should be derived from the bucket name
		})
	}

	invalid := []string{
		"",
		"bucket.with.dots",
		"bucket with spaces",
		"bucket/with/slashes",
		"bucket*",
		"bucket>",
	}
	for _, name := range invalid {
		t.Run("invalid/"+name, func(t *testing.T) {
			is := is.New(t)
			_, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: name})
			is.True(err != nil)                                           // invalid bucket name should be rejected
			is.True(strings.Contains(err.Error(), "invalid bucket name")) // error should identify the invalid bucket name
		})
	}
}

func TestCreateBucketRejectsConflictingConfig(t *testing.T) {
	tests := []struct {
		name       string
		config     jetstream.StreamConfig
		errorMatch string
	}{
		{
			name:       "stream name",
			config:     jetstream.StreamConfig{Name: "OTHER"},
			errorMatch: "StreamConfig.Name must not be set",
		},
		{
			name:       "history greater than one",
			config:     jetstream.StreamConfig{MaxMsgsPerSubject: 2},
			errorMatch: "history) must be 1",
		},
		{
			name:       "unlimited history",
			config:     jetstream.StreamConfig{MaxMsgsPerSubject: -1},
			errorMatch: "history) must be 1",
		},
		{
			name:       "interest retention",
			config:     jetstream.StreamConfig{Retention: jetstream.InterestPolicy},
			errorMatch: "Retention must be LimitsPolicy",
		},
		{
			name:       "work queue retention",
			config:     jetstream.StreamConfig{Retention: jetstream.WorkQueuePolicy},
			errorMatch: "Retention must be LimitsPolicy",
		},
		{
			name:       "negative subject delete marker TTL",
			config:     jetstream.StreamConfig{SubjectDeleteMarkerTTL: -time.Second},
			errorMatch: "SubjectDeleteMarkerTTL must be zero or at least 1s",
		},
		{
			name:       "short subject delete marker TTL",
			config:     jetstream.StreamConfig{SubjectDeleteMarkerTTL: 500 * time.Millisecond},
			errorMatch: "SubjectDeleteMarkerTTL must be zero or at least 1s",
		},
	}

	nc := testConnection(t)
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			cfg := nkv.Config{
				Bucket:       "CONFLICT_" + strconv.Itoa(index),
				StreamConfig: test.config,
			}
			_, err := nkv.CreateBucket(t.Context(), nc, cfg)
			is.True(err != nil)                                     // conflicting stream configuration should be rejected
			is.True(strings.Contains(err.Error(), test.errorMatch)) // error should describe the conflicting field
		})
	}
}

func TestCreateBucketConfig(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)

	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{
		Bucket: "CONFIG",
		StreamConfig: jetstream.StreamConfig{
			Description:            "configured bucket",
			Subjects:               []string{"OTHER.>"},
			MaxAge:                 time.Hour,
			MaxBytes:               1024,
			Metadata:               map[string]string{"owner": "nkv"},
			Discard:                jetstream.DiscardOld,
			SubjectDeleteMarkerTTL: 2 * time.Minute,
		},
	})
	is.NoErr(err) // bucket creation with supported settings should succeed

	status, err := kv.Status(t.Context())
	is.NoErr(err) // created bucket status should be available
	config := status.Config

	is.Equal(config.Name, "KV_CONFIG")                     // stream name should follow the KV wire layout
	is.Equal(config.Subjects, []string{"$KV.CONFIG.>"})    // subjects should follow the KV wire layout
	is.Equal(config.Description, "configured bucket")      // description should be preserved
	is.Equal(config.MaxAge, time.Hour)                     // maximum age should be preserved
	is.Equal(config.MaxBytes, int64(1024))                 // maximum bytes should be preserved
	is.Equal(config.Metadata["owner"], "nkv")              // metadata should be preserved
	is.Equal(config.Retention, jetstream.LimitsPolicy)     // KV streams should use limits retention
	is.Equal(config.Discard, jetstream.DiscardNew)         // KV streams should reject writes over limits
	is.Equal(config.MaxMsgsPerSubject, int64(1))           // KV streams should retain one revision per key
	is.Equal(config.Replicas, 1)                           // bucket should default to one replica
	is.Equal(config.Storage, jetstream.FileStorage)        // bucket should default to file storage
	is.Equal(config.Duplicates, 2*time.Minute)             // bucket should default the duplicate window
	is.True(config.AllowRollup)                            // KV streams should allow purge rollups
	is.True(config.DenyDelete)                             // KV streams should deny direct message deletion
	is.True(config.AllowDirect)                            // KV streams should allow direct reads
	is.True(config.AllowMsgTTL)                            // KV streams should allow per-message TTL
	is.Equal(config.SubjectDeleteMarkerTTL, 2*time.Minute) // caller-supplied marker TTL should be preserved
	is.True(config.AllowAtomicPublish)                     // KV streams should allow atomic publishing
}

func TestCreateBucketDefaultsSubjectDeleteMarkerTTL(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)

	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "MARKER_TTL_DEFAULT"})
	is.NoErr(err) // bucket creation should succeed

	status, err := kv.Status(t.Context())
	is.NoErr(err)                                               // created bucket status should be available
	is.Equal(status.Config.SubjectDeleteMarkerTTL, time.Minute) // marker TTL should default to one minute
}

func TestCreateBucketUpdatesExistingBucket(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)

	_, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{
		Bucket:       "UPDATE",
		StreamConfig: jetstream.StreamConfig{Description: "before"},
	})
	is.NoErr(err) // initial bucket creation should succeed

	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{
		Bucket:       "UPDATE",
		StreamConfig: jetstream.StreamConfig{Description: "after"},
	})
	is.NoErr(err) // creating an existing bucket should update it

	status, err := kv.Status(t.Context())
	is.NoErr(err)                                // updated bucket status should be available
	is.Equal(status.Config.Description, "after") // updated description should be persisted
}

func TestOpenBucket(t *testing.T) {
	nc := testConnection(t)
	js, err := jetstream.New(nc)
	is.New(t).NoErr(err) // JetStream client creation should succeed

	_, err = nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "OPEN"})
	is.New(t).NoErr(err) // bucket creation should succeed

	tests := []struct {
		name       string
		bucket     string
		wantErr    string
		wantStream string
	}{
		{name: "existing bucket", bucket: "OPEN", wantStream: "KV_OPEN"},
		{name: "invalid name", bucket: "INVALID.NAME", wantErr: "invalid bucket name"},
		{name: "missing bucket", bucket: "MISSING", wantErr: "stream not found"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			kv, err := nkv.Open(t.Context(), nc, test.bucket)
			if test.wantErr != "" {
				is.True(err != nil)                                  // invalid or unavailable bucket should not open
				is.True(strings.Contains(err.Error(), test.wantErr)) // open error should describe the failure
				return
			}
			is.NoErr(err)                          // compatible existing bucket should open
			is.Equal(kv.Name(), test.bucket)       // opened bucket should retain its name
			is.Equal(kv.Stream(), test.wantStream) // opened bucket should expose its backing stream
		})
	}

	compatibleConfig := func(bucket string) jetstream.StreamConfig {
		return jetstream.StreamConfig{
			Name:                   "KV_" + bucket,
			Subjects:               []string{"$KV." + bucket + ".>"},
			Retention:              jetstream.LimitsPolicy,
			Discard:                jetstream.DiscardNew,
			MaxMsgsPerSubject:      1,
			AllowRollup:            true,
			DenyDelete:             true,
			AllowDirect:            true,
			AllowMsgTTL:            true,
			SubjectDeleteMarkerTTL: time.Minute,
			AllowAtomicPublish:     true,
		}
	}

	incompatible := []struct {
		name    string
		bucket  string
		wantErr string
		mutate  func(*jetstream.StreamConfig)
	}{
		{name: "subjects", bucket: "OPEN_SUBJECTS", wantErr: "incompatible subjects", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.Subjects = []string{"OTHER.>"}
		}},
		{name: "rollup", bucket: "OPEN_ROLLUP", wantErr: "allow_rollup=false", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.AllowRollup = false
			cfg.SubjectDeleteMarkerTTL = 0
		}},
		{name: "deny delete", bucket: "OPEN_DELETE", wantErr: "deny_delete=false", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.DenyDelete = false
		}},
		{name: "direct get", bucket: "OPEN_DIRECT", wantErr: "allow_direct=false", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.AllowDirect = false
		}},
		{name: "message TTL", bucket: "OPEN_TTL", wantErr: "allow_msg_ttl=false", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.AllowMsgTTL = false
			cfg.SubjectDeleteMarkerTTL = 0
		}},
		{name: "marker TTL", bucket: "OPEN_MARKER_TTL", wantErr: "subject_delete_marker_ttl=0s", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.SubjectDeleteMarkerTTL = 0
		}},
		{name: "atomic publish", bucket: "OPEN_ATOMIC", wantErr: "allow_atomic_publish=false", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.AllowAtomicPublish = false
		}},
		{name: "discard", bucket: "OPEN_DISCARD", wantErr: "does not discard new messages", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.Discard = jetstream.DiscardOld
		}},
		{name: "retention", bucket: "OPEN_RETENTION", wantErr: "does not use limits retention", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.Retention = jetstream.InterestPolicy
		}},
		{name: "history", bucket: "OPEN_HISTORY", wantErr: "max_msgs_per_subject=2", mutate: func(cfg *jetstream.StreamConfig) {
			cfg.MaxMsgsPerSubject = 2
		}},
	}

	for _, test := range incompatible {
		t.Run("incompatible "+test.name, func(t *testing.T) {
			is := is.New(t)
			cfg := compatibleConfig(test.bucket)
			test.mutate(&cfg)
			_, err := js.CreateStream(t.Context(), cfg)
			is.NoErr(err) // incompatible stream fixture should be created
			_, err = nkv.Open(t.Context(), nc, test.bucket)
			is.True(err != nil)                                  // incompatible stream should not open
			is.True(strings.Contains(err.Error(), test.wantErr)) // error should identify the incompatible setting
		})
	}
}

func TestBucketStatusAndLocality(t *testing.T) {
	is := is.New(t)
	nc := testConnection(t)
	kv, err := nkv.CreateBucket(t.Context(), nc, nkv.Config{Bucket: "STATUS"})
	is.NoErr(err) // bucket creation should succeed

	local, err := kv.IsClusterLocal(t.Context())
	is.NoErr(err)  // locality check should succeed
	is.True(local) // bucket on a standalone server should be local

	js, err := jetstream.New(nc)
	is.NoErr(err)                                       // JetStream client creation should succeed
	is.NoErr(js.DeleteStream(t.Context(), kv.Stream())) // backing stream deletion should succeed

	_, err = kv.Status(t.Context())
	is.True(err != nil) // status should fail after the backing stream is deleted
}
