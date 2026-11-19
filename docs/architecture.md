# RudderStack Provider Architecture

## Overview

The RudderStack provider turns platform activity into product analytics. It
consumes Kubernetes audit events from NATS JetStream, evaluates them against
`TrackingRule` resources, and sends the resulting `track`, `identify` and
`group` calls to RudderStack.

It follows the design of the [search] resource indexer: a durable JetStream
consumer, policy resources with CEL conditions compiled once and cached, and
batches that are acknowledged only after the backend accepts them. Unlike
search, most rules need state (counts, "already fired" markers, last-seen
times), which is kept in NATS KV.

[search]: https://github.com/milo-os/search/blob/main/docs/components/resource-indexer.md

```
AUDIT_EVENTS stream ─▶ consumer ─▶ rule engine ─▶ batcher ─▶ RudderStack /v1/batch
                                     │    ▲                    (ack after 2xx)
                                     ▼    │
                                   NATS KV: members, fired markers, last-seen
sweeper (timer) ─▶ Inactivity / Frequency rules ─▶ batcher
```

## Why audit events, not activities

Activities are the human-readable output of the activity processor, but an
activity produced from an audit log does not record the verb. Without it the
provider cannot tell "domain added" from "domain removed", which threshold
counting depends on. Audit events also carry the `userAgent` (needed for
datumctl rules), the authenticated user, the request scope and the object.

## TrackingRule

`TrackingRule` (`analytics.miloapis.com/v1alpha1`, cluster-scoped) maps audit
events to one RudderStack call. Each row of the analytics event list is one
rule, so events are added through GitOps without a code change. See
[`config/samples`](../config/samples) for the current rules.

| Field | Purpose |
|---|---|
| `event` | RudderStack event name, Object + Action (`Domain Added`) |
| `call` | `Track` (default), `Identify` or `Group` |
| `match.resource` | `{group, resource}` as audit events name it (plural resource) |
| `match.verbs` | Audit verbs to match |
| `match.conditions` | CEL, OR semantics |
| `subject` | `User` (default) or `Organization`: who counts and fires |
| `userId` | CEL override for the RudderStack userId |
| `trigger` | `Every`, `Once`, `Threshold`, `Inactivity` or `Frequency` |
| `properties` | CEL values sent with the call; nothing else leaves the platform |

CEL expressions can read `verb`, `user`, `userAgent`, `objectRef`, `object`
and `tenant`. The controller compiles every expression and sets the rule's
`Ready` condition; rules that are not Ready are not evaluated.

### Triggers

| Trigger | Fires | State |
|---|---|---|
| `Every` | On every matching event | None |
| `Once` | The first time a subject matches ("first domain") | `fired.<rule>.<subject>` |
| `Threshold` | When the subject first holds `atLeast` distinct resources | `members.<rule>.<subject>` (set of resource UIDs), fired marker |
| `Inactivity` | When the subject is unseen for `after` | `lastseen.<subject>`, fired marker per inactive period |
| `Frequency` | When the subject is seen on `days` distinct days in `window` | Per-day activity set |

Threshold counts are sets of resource UIDs, not counters: a create adds the
UID and a delete removes it, so a redelivered message cannot drift the count.

## Components

| Package | Role |
|---|---|
| `internal/audit` | Decodes audit events; filters failed, dry-run and incomplete requests |
| `internal/rules` | Compiles rules, evaluates them, and keeps an informer-fed store on every replica |
| `internal/pipeline` | JetStream consumer, event processor and batcher |
| `internal/rudderstack` | `/v1/batch` client and the dry-run sender |
| `internal/controller` | Sets each rule's Ready condition (leader only) |

The consumer waits for the rule store to sync before it consumes, so events
are never evaluated against a partial rule set. The pod reports ready once it
is consuming.

## Delivery guarantees

- Messages are acknowledged only after RudderStack returns 2xx for every call
  they produced. Network errors, `429` and `5xx` nack the message with a 10s
  delay.
- Memory is bounded by the JetStream consumer's `maxAckPending`, since
  unacked messages are the only thing the batcher holds.
- At startup the provider refuses to run when the consumer's `ackWait` is
  shorter than the flush interval plus two send timeouts.
- A batch RudderStack rejects with `4xx` is split in half repeatedly until
  the rejected calls are isolated. Those are dropped and counted; a
  dead-letter subject replaces dropping in phase 4.
- Every call has a deterministic `messageId`, so RudderStack drops duplicates
  from redelivery:
  - `Every`: the audit ID and rule name.
  - `Once` and `Threshold`: `<rule>/<subject>`.
  - `Inactivity`: `<rule>/<subject>/<period>`.
- Fired markers are written after a successful send. A crash between the two
  resends the call with the same `messageId`.
- The SDK (`analytics-go`) is not used: it buffers and sends on its own
  schedule, which breaks acknowledge-after-delivery.

## Identity

- `userId` is the acting user's uid unless the rule sets `userId`.
- Sign-up sends `identify` with the user's email, which is approved for
  RudderStack.
- Organization subjects are resolved from the request's project scope through
  an informer on Milo `Project` resources. Calls carry `context.groupId`.

## Rollout

- The JetStream consumer starts with `deliverPolicy: new` so history is not
  replayed into analytics.
- Before rules go live in production, a one-off job seeds the `members` sets
  and fired markers from current cluster state without sending events.
  Otherwise every existing customer receives "first X" events the next time
  they act.
- `--dry-run` logs payloads instead of sending them; staging runs this way
  first.

## Phases

1. **Done:** the `TrackingRule` CRD, CEL compilation with the Ready
   condition, and the sample rules.
2. **Done:** the audit consumer, `Every` rules with `Track` and `Identify`
   calls, the RudderStack sink and dry-run mode. Rules with other triggers
   are skipped and counted in `rudderstack_provider_rules_skipped_total`.
3. NATS KV state: `Once`, `Threshold`, organization resolution, `identify`
   and `group`.
4. The sweeper (`Inactivity`, `Frequency`), the dead-letter path, dashboards.
5. Infra: NACK consumer and KV buckets, NATS client certificate, write key
   ExternalSecret, Milo CRD installation, and the seed job.

## Open items

| Event | Status |
|---|---|
| Payment information completed | Billing resource to confirm |
| Desktop app downloaded | Not visible to the control plane; track on the download page |
| Add DNS Zone, zone thresholds | DNSZone group/resource to confirm |
| ALB added | `NetworkLoadBalancer` or `Gateway` to confirm |
| More than 4 tunnels | Confirm tunnels are `Connector` resources |
| Frequent / active datumctl user | Definition needed; samples use 5 days in 14 |
| Galactic usage | Definition needed |
