# RudderStack Provider

Sends product analytics to [RudderStack] from Milo platform activity. The
provider consumes Kubernetes audit events from NATS JetStream, evaluates them
against `TrackingRule` resources, and sends `track`, `identify` and `group`
calls. Milestones are computed downstream by scheduled queries in BigQuery.
It shares its design with [search].

See [docs/architecture.md](docs/architecture.md) for the design.

[RudderStack]: https://www.rudderstack.com
[search]: https://github.com/milo-os/search

## TrackingRule

```yaml
apiVersion: analytics.miloapis.com/v1alpha1
kind: TrackingRule
metadata:
  name: domain-added
spec:
  event: Domain Added
  match:
    resource:
      group: networking.datumapis.com
      resource: domains
    verbs: [create]
  properties:
    - name: name
      expression: objectRef.name
    - name: changeSource
      expression: 'actor.kind == "user" ? "human" : actor.kind == "serviceaccount" ? "automation" : "system"'
```

Milestones such as "first domain" or "more than 3 domains" are computed from
these events by scheduled queries in BigQuery, not by the provider.

The rules for the analytics event list are in [config/rules](config/rules).

## Running

| Flag | Default | Purpose |
|---|---|---|
| `--nats-url` | | NATS server. Without it only the TrackingRule controller runs |
| `--nats-tls-cert`, `--nats-tls-key`, `--nats-tls-ca` | | NATS client TLS |
| `--nats-stream` | `AUDIT_EVENTS` | Stream carrying audit events |
| `--nats-consumer` | `rudderstack-provider` | Durable consumer, declared outside the provider |
| `--nats-kv-bucket` | `rudderstack-provider-limits` | KV bucket for once-per-day limits, declared outside the provider with a TTL of at least 24h |
| `--service-account-email-suffix` | | Domain suffix of customer machine account emails, seen by rules as `actor.kind == "serviceaccount"` |
| `--rudderstack-data-plane-url` | | RudderStack data plane |
| `--dry-run` | `false` | Log calls instead of sending them |
| `--batch-max-calls` | `100` | Calls per request |
| `--batch-max-bytes` | `4000000` | Request body limit |
| `--batch-flush-interval` | `5s` | Longest a call waits before it is sent |
| `--rudderstack-send-timeout` | `30s` | Timeout for one request |

The write key is read from `$RUDDERSTACK_WRITE_KEY`. Metrics are prefixed
`rudderstack_provider_`.

In dry run, each call is logged instead of sent. The log line holds the call's
type, event, `messageId`, `groupId`, timestamp, the names of its properties and
traits, and its size in bytes. It never holds property or trait values, such as
emails or organization display names. The `userId` is logged only as a short
SHA-256 hash, because it can be a username, which is often an email address.

The deployment bundle, and what an environment has to supply, is described in
[docs/architecture.md](docs/architecture.md#deployment).

## Development

| Command | Purpose |
|---|---|
| `make test` | Generate, vet and run tests (envtest) |
| `make lint` | Run golangci-lint |
| `make build` | Build `bin/manager` |
| `make run` | Run against the cluster in `~/.kube/config` |
| `make manifests generate` | Regenerate the CRD, RBAC and deepcopy code after changing API types |

The project was scaffolded with Kubebuilder v4 (see `PROJECT`), so
`kubebuilder create api` and `kubebuilder create webhook` work as usual.
