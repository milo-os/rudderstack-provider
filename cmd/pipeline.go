package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	ctrl "sigs.k8s.io/controller-runtime"

	"go.miloapis.net/rudderstack-provider/internal/limit"
	"go.miloapis.net/rudderstack-provider/internal/orgs"
	"go.miloapis.net/rudderstack-provider/internal/pipeline"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
	"go.miloapis.net/rudderstack-provider/internal/rules"
)

// writeKeyEnv holds the RudderStack source write key. It is read from the
// environment so it can come from a Secret without appearing in the pod spec.
const writeKeyEnv = "RUDDERSTACK_WRITE_KEY"

// pipelineOptions configures the audit event pipeline.
type pipelineOptions struct {
	natsURL      string
	natsTLSCert  string
	natsTLSKey   string
	natsTLSCA    string
	natsStream   string
	natsConsumer string
	natsKVBucket string

	dataPlaneURL string
	dryRun       bool

	serviceAccountEmailSuffix string

	excludeUsers    string
	excludeProjects string
	excludeOrgs     string

	batch pipeline.BatchConfig
}

func (o *pipelineOptions) bindFlags(fs *flag.FlagSet) {
	fs.StringVar(&o.natsURL, "nats-url", "",
		"NATS server URL. When empty, only the TrackingRule controller runs and no events are consumed.")
	fs.StringVar(&o.natsTLSCert, "nats-tls-cert", "", "Client certificate for NATS.")
	fs.StringVar(&o.natsTLSKey, "nats-tls-key", "", "Client key for NATS.")
	fs.StringVar(&o.natsTLSCA, "nats-tls-ca", "", "CA bundle that signs the NATS server certificate.")
	fs.StringVar(&o.natsStream, "nats-stream", "AUDIT_EVENTS", "JetStream stream carrying audit events.")
	fs.StringVar(&o.natsConsumer, "nats-consumer", "rudderstack-provider",
		"Durable JetStream consumer to read from. It is declared outside the provider.")
	fs.StringVar(&o.natsKVBucket, "nats-kv-bucket", "rudderstack-provider-limits",
		"NATS KV bucket for once-per-day limits. It is declared outside the provider, with a TTL of at least 24h.")
	fs.StringVar(&o.dataPlaneURL, "rudderstack-data-plane-url", "",
		"RudderStack data plane URL. The write key is read from $"+writeKeyEnv+".")
	fs.BoolVar(&o.dryRun, "dry-run", false, "Log RudderStack calls instead of sending them.")
	fs.StringVar(&o.serviceAccountEmailSuffix, "service-account-email-suffix", "",
		"Domain suffix of customer machine account emails, which rules see as actor.kind \"serviceaccount\". "+
			"When empty, machine accounts count as users.")
	fs.StringVar(&o.excludeUsers, "exclude-users", "",
		"Comma-separated user uids or usernames whose activity, and calls about them, are never sent.")
	fs.StringVar(&o.excludeProjects, "exclude-projects", "",
		"Comma-separated project names whose activity is never sent.")
	fs.StringVar(&o.excludeOrgs, "exclude-orgs", "",
		"Comma-separated organization names whose activity, and calls about them, are never sent.")
	fs.IntVar(&o.batch.MaxCalls, "batch-max-calls", 100, "Most calls in one request to RudderStack.")
	fs.IntVar(&o.batch.MaxBytes, "batch-max-bytes", rudderstack.MaxBatchBytes,
		"Largest request body sent to RudderStack. Capped at RudderStack's limit.")
	fs.DurationVar(&o.batch.FlushInterval, "batch-flush-interval", 5*time.Second,
		"Longest a call waits before it is sent.")
	fs.DurationVar(&o.batch.SendTimeout, "rudderstack-send-timeout", 30*time.Second,
		"Timeout for one request to RudderStack.")
}

func (o *pipelineOptions) enabled() bool { return o.natsURL != "" }

func (o *pipelineOptions) validate() error {
	if !o.enabled() {
		return nil
	}
	var errs []error
	if !o.dryRun && !strings.HasPrefix(o.dataPlaneURL, "https://") {
		// The write key is sent with every request.
		errs = append(errs, errors.New("--rudderstack-data-plane-url must be an https:// URL unless --dry-run is set"))
	}
	if !o.dryRun && os.Getenv(writeKeyEnv) == "" {
		errs = append(errs, fmt.Errorf("$%s is required unless --dry-run is set", writeKeyEnv))
	}
	if (o.natsTLSCert == "") != (o.natsTLSKey == "") {
		errs = append(errs, errors.New("--nats-tls-cert and --nats-tls-key must be set together"))
	}
	if o.batch.MaxCalls < 1 {
		errs = append(errs, errors.New("--batch-max-calls must be at least 1"))
	}
	if o.batch.FlushInterval <= 0 || o.batch.SendTimeout <= 0 {
		errs = append(errs, errors.New("--batch-flush-interval and --rudderstack-send-timeout must be positive"))
	}
	return errors.Join(errs...)
}

// setupPipeline connects to NATS and adds the consumer and batcher to mgr.
// The returned function closes the NATS connection.
func setupPipeline(ctx context.Context, mgr ctrl.Manager, o *pipelineOptions, store *rules.Store) (func(), error) {
	resolver := orgs.NewResolver()
	if err := resolver.SetupWithManager(ctx, mgr); err != nil {
		return nil, fmt.Errorf("watching projects: %w", err)
	}

	log := ctrl.Log.WithName("pipeline")

	natsOpts := []nats.Option{nats.Name("rudderstack-provider"), nats.MaxReconnects(-1)}
	if o.natsTLSCA != "" {
		natsOpts = append(natsOpts, nats.RootCAs(o.natsTLSCA))
	}
	if o.natsTLSCert != "" {
		natsOpts = append(natsOpts, nats.ClientCert(o.natsTLSCert, o.natsTLSKey))
	}
	nc, err := nats.Connect(o.natsURL, natsOpts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS: %w", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	cons, err := js.Consumer(ctx, o.natsStream, o.natsConsumer)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("getting consumer %s on stream %s: %w", o.natsConsumer, o.natsStream, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("getting info for consumer %s: %w", o.natsConsumer, err)
	}
	if err := checkAckWait(info.Config.AckWait, o.batch); err != nil {
		nc.Close()
		return nil, err
	}

	bucket, err := js.KeyValue(ctx, o.natsKVBucket)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("getting KV bucket %s: %w", o.natsKVBucket, err)
	}
	status, err := bucket.Status(ctx)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("getting status of KV bucket %s: %w", o.natsKVBucket, err)
	}
	if err := checkBucketTTL(status.TTL()); err != nil {
		nc.Close()
		return nil, err
	}

	if o.serviceAccountEmailSuffix == "" {
		log.Info("No --service-account-email-suffix set; customer machine accounts count as users")
	}
	exclude := o.exclusions()
	// Counts only: excluded usernames can be email addresses.
	log.Info("Excluded accounts", "users", len(exclude.Users), "projects", len(exclude.Projects), "orgs", len(exclude.Orgs))

	var sender rudderstack.Sender = rudderstack.NewClient(o.dataPlaneURL, os.Getenv(writeKeyEnv), o.batch.SendTimeout)
	if o.dryRun {
		log.Info("Dry run: RudderStack calls are logged, not sent")
		sender = rudderstack.DryRunSender{Log: log}
	}

	batcher := pipeline.NewBatcher(sender, o.batch, log.WithName("batcher"))
	consumer := &pipeline.Consumer{
		JetStream: cons,
		Processor: &pipeline.Processor{
			Rules:   store,
			Batcher: batcher,
			Orgs:    resolver,
			Limiter: limit.NewCached(&limit.KV{Bucket: bucket}),
			Log:     log.WithName("processor"),

			ServiceAccountEmailSuffix: o.serviceAccountEmailSuffix,
			Exclude:                   exclude,
		},
		Synced: func() bool { return store.HasSynced() && resolver.HasSynced() },
		Log:    log,
	}
	if err := errors.Join(
		mgr.Add(batcher),
		mgr.Add(consumer),
		mgr.AddReadyzCheck("consumer", consumer.ReadyCheck),
	); err != nil {
		nc.Close()
		return nil, err
	}
	return nc.Close, nil
}

func (o *pipelineOptions) exclusions() pipeline.Exclusions {
	return pipeline.NewExclusions(
		strings.Split(o.excludeUsers, ","),
		strings.Split(o.excludeProjects, ","),
		strings.Split(o.excludeOrgs, ","),
	)
}

// checkBucketTTL fails when the limits bucket would keep keys forever, or
// expire them before the day they cover is over.
func checkBucketTTL(ttl time.Duration) error {
	if ttl < limit.MinTTL {
		return fmt.Errorf("KV bucket TTL is %s; it must be at least %s so once-per-day keys last the whole day "+
			"and are then removed", ttl, limit.MinTTL)
	}
	return nil
}

// checkAckWait fails when JetStream could redeliver a message while it is
// still waiting in a batch: a call can wait one flush interval and then one
// send before its message is acked.
func checkAckWait(ackWait time.Duration, cfg pipeline.BatchConfig) error {
	if need := cfg.FlushInterval + 2*cfg.SendTimeout; ackWait < need {
		return fmt.Errorf("consumer ackWait %s is shorter than the %s a message can wait for delivery "+
			"(flush interval + 2 x send timeout); raise ackWait or lower the batch settings", ackWait, need)
	}
	return nil
}
