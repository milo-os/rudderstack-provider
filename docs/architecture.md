# RudderStack Provider Architecture

## Overview

The RudderStack provider turns platform activity into product analytics. It
consumes Kubernetes audit events from NATS JetStream, evaluates them against
`TrackingRule` resources, and sends `track`, `identify` and `group` calls to
RudderStack.

The provider forwards events and keeps almost no state. Milestones such as
"first domain", "more than 3 domains" or "inactive for 14 days" are not
computed here. RudderStack loads every event into BigQuery, and RudderStack
Profiles computes the milestones from the full history there. The results go
to marketing tools as traits and audiences.

It follows the design of the [search] resource indexer: a durable JetStream
consumer, policy resources with CEL conditions compiled once and cached, and
batches that are acknowledged only after the backend accepts them.

[search]: https://github.com/milo-os/search/blob/main/docs/components/resource-indexer.md

```
AUDIT_EVENTS ─▶ rudderstack-provider ─▶ RudderStack ─▶ BigQuery (product_analytics)
 (NATS)          match rules, send                       │
                 raw events                              ▼
                                               RudderStack Profiles
                                         (milestones, counts, inactivity)
                                                         │
                                                         ▼
                                       traits / audiences ─▶ marketing tool
```

## Responsibilities

| Concern | Owner |
|---|---|
| Which platform actions become events, and their properties | `TrackingRule` resources (this repo) |
| Reliable delivery, deduplication of redelivered events | Provider |
| Daily activity signals (`Active`, `datumctl Used`) without flooding RudderStack | Provider (once-per-day limit) |
| Event storage and history | BigQuery, loaded by RudderStack |
| "First X", "more than N", "inactive for N days", "frequent user" | RudderStack Profiles |
| Sending milestones to marketing tools | RudderStack (audiences / reverse ETL) |

This split keeps milestone definitions where the people who tune them work.
Changing "more than 3 domains" to "more than 5" is a Profiles change, not a
provider release. Profiles also computes from the complete history, which
rules out drift from missed or duplicated events.

## Why audit events, not activities

Activities are the human-readable output of the activity processor, but an
activity produced from an audit log does not record the verb. Without it the
provider cannot tell "domain added" from "domain removed", and the removal
events are what let Profiles count resources that currently exist. Audit
events also carry the `userAgent` (needed for datumctl signals), the
authenticated user, the request scope and the object.

## TrackingRule

`TrackingRule` (`analytics.miloapis.com/v1alpha1`, cluster-scoped) maps audit
events to one RudderStack call. Rules are added through GitOps without a code
change. See [`config/samples`](../config/samples).

| Field | Purpose |
|---|---|
| `event` | RudderStack event name, Object + Action (`Domain Added`) |
| `call` | `Track` (default), `Identify` or `Group` |
| `match.resource` | `{group, resource}` as audit events name it (plural resource) |
| `match.verbs` | Audit verbs to match |
| `match.conditions` | CEL, OR semantics |
| `subject` | `User` (default) or `Organization` |
| `userId` | CEL override for the RudderStack userId |
| `groupId` | CEL override for the RudderStack groupId; defaults to the request's organization |
| `limit` | Optional `OncePerDay`: send at most one call per subject per UTC day |
| `properties` | CEL values sent with the call; nothing else leaves the platform |

CEL expressions can read `verb`, `user`, `userAgent`, `objectRef`, `object`
and `tenant`. The controller compiles every expression and sets the rule's
`Ready` condition; rules that are not Ready are not evaluated.

### Event catalog

Every "added" event has a matching "removed" event so Profiles can compute
current counts. These rules match every actor, not only humans: resources are
often removed by the system (deleting a project makes the garbage collector
delete its resources), and a missed removal would leave a count too high for
good. Each event carries:

- `name`, `namespace` and `project`, which identify the resource, so Profiles
  matches a removal to its add regardless of who made either.
- `changeSource` (`human` or `system`), so milestones such as "user removes
  domains" can count only human actions.

| Event | Source | Feeds |
|---|---|---|
| `Signed Up` + `identify` (email) | `iam.miloapis.com/users` create | Sign up |
| `Project Created` / `Project Deleted` | `resourcemanager.miloapis.com/projects` | First project |
| `Domain Added` / `Domain Removed` | `networking.datumapis.com/domains` | First, >3 (user), >10 (org), removes domains |
| `Proxy Added` / `Proxy Removed` | `networking.datumapis.com/httpproxies` | First proxy |
| `Workload Deployed` / `Workload Removed` | `compute.datumapis.com/workloads` | Deployed compute, >10 workloads, spins down compute |
| `Contributor Added` / `Contributor Removed` | `resourcemanager.miloapis.com/organizationmemberships` | Add contributor, >10 teammates |
| `DNS Zone Added` / `DNS Zone Removed` | DNS zone resource (to confirm) | First zone, >5, >15 org, removes zones |
| `Load Balancer Added` / `Load Balancer Removed` | To confirm | ALB added |
| `Tunnel Added` / `Tunnel Removed` | To confirm (`Connector`?) | >4 active tunnels |
| `datumctl Used` (once per day) | Any request by a human user with a `datumctl/` user agent | First datumctl access, frequent / active datumctl user |
| `Active` (once per day) | Any request by a human user | Inactivity > 14 days |
| `group` (`name`, `displayName`) | `resourcemanager.miloapis.com/organizations` create/update | Org-level milestones |

### Milestones in Profiles

Illustrative definitions. The Profiles project defines them precisely.

| Milestone | Definition over BigQuery events |
|---|---|
| First domain | `min(timestamp)` of `Domain Added` per user |
| > 3 domains (user) | Domains the user added, keyed by `project`/`name`, with no later `Domain Removed`, > 3 |
| > 10 domains (org) | Same per `context.groupId` > 10 |
| > 4 active tunnels | Tunnels added with no later `Tunnel Removed` > 4 |
| Removes domains | `Domain Removed` with `changeSource = human` |
| Frequent datumctl user | Distinct days with `datumctl Used` in the last 14 ≥ N |
| Inactive > 14 days | `max(timestamp)` of `Active` older than 14 days |

## Delivery guarantees

- Messages are acknowledged only after RudderStack returns 2xx for every call
  they produced. Network errors, `429` and `5xx` nack the message with a 10s
  delay.
- Every call has a deterministic `messageId`, so RudderStack drops duplicates
  from redelivery: `<auditID>/<rule>`, or `<rule>/<subject>/<date>` for
  once-per-day rules.
- A batch RudderStack rejects with `4xx` is split in half repeatedly until
  the rejected calls are isolated. Those are dropped and counted.
- Memory is bounded by the JetStream consumer's `maxAckPending`, since
  unacked messages are the only thing the batcher holds.
- At startup the provider refuses to run when the consumer's `ackWait` is
  shorter than the flush interval plus two send timeouts.

## State

The only state is the once-per-day limit: a NATS KV bucket holding one key
per rule, subject and UTC day (the subject is hashed, since user IDs can hold
characters KV keys do not allow). Its TTL must be at least 24 hours; the
provider refuses to start otherwise. A key is created atomically before the
call is queued, and only the first creation sends.

Every failure mode sends rather than drops, and relies on the call's
`<rule>/<subject>/<date>` `messageId` for RudderStack to deduplicate:

- A redelivered message skips the limiter. Its first delivery already took
  the key, but its send failed.
- If the bucket is unavailable, the call is sent and counted in
  `rudderstack_provider_limit_errors_total`.
- Losing the bucket sends at most one extra call per subject that day.

The limit exists for cost and volume. RudderStack bills by event, and
forwarding every datumctl or console request would multiply volume for no
analytical gain.

## Components

| Package | Role |
|---|---|
| `internal/audit` | Decodes audit events; filters failed, dry-run and incomplete requests |
| `internal/rules` | Compiles rules, evaluates them, and keeps an informer-fed store on every replica |
| `internal/pipeline` | JetStream consumer, event processor and batcher |
| `internal/rudderstack` | `/v1/batch` client and the dry-run sender |
| `internal/limit` | Once-per-day limiter on NATS KV |
| `internal/orgs` | Resolves a request's organization, keeping a project-to-organization map from an informer on Milo `Project` resources |
| `internal/controller` | Sets each rule's Ready condition (leader only) |

The consumer waits for the rule store and the project map to sync before it
consumes, so events are never evaluated against a partial rule set or sent
without an organization the provider could have known. The pod reports ready once it
is consuming.

## Identity

- `userId` is the acting user's uid unless the rule sets `userId`.
- Sign-up sends `identify` with the user's email, which is approved for
  RudderStack.
- `groupId` is the organization the request was made in: the scope itself
  when it is an organization, or the project's owner when it is a project. A
  rule's `groupId` expression overrides it, which `group` calls for a new
  organization need, since that request is not made inside it.
- `track` and `identify` calls carry the organization as `context.groupId`;
  `group` calls carry it as `groupId` with the rule's properties as traits.
- A project the provider has not seen yet (created moments before) leaves
  `groupId` empty and is counted in `rudderstack_provider_orgs_unresolved_total`.
  The event is still sent: Profiles can recover the organization from the
  project's `Project Created` event.
- Rules with an `Organization` subject, and `group` calls, are skipped when
  there is no organization.

## Backfill

Counts in Profiles need the resources customers created before launch. A
one-off job lists existing resources and sends their "added" events with the
resource's `creationTimestamp` as `originalTimestamp` and `backfill/<uid>` as
`messageId`, plus `identify` and `group` for existing users and
organizations. It runs once, before milestones are switched on for marketing.

## Rollout

- The JetStream consumer starts with `deliverPolicy: new` so history is not
  replayed into analytics.
- `--dry-run` logs payloads instead of sending them; staging runs this way
  first.

## Phases

| # | Scope | Status |
|---|---|---|
| 1 | Scaffold, `TrackingRule` CRD, CEL compilation and Ready condition, samples | Done (#3) |
| 2 | Audit consumer, `Every` rules, RudderStack sink, dry-run | Done (#4) |
| 3 | Replace triggers with the optional `limit`; add the "removed" rules; update samples to the event catalog | Done (#4) |
| 4 | Once-per-day limit (NATS KV), which turns on `datumctl Used` and `Active`; organization resolution and `group` calls | Done |
| 5 | Infra: BigQuery `product_analytics` dataset, RudderStack loader service account and GCS staging bucket (Pulumi, `infra/gcp`); NACK consumer and KV bucket (TTL ≥ 24h), NATS client certificate, write key ExternalSecret, Milo CRD, RBAC to watch projects; staging in dry-run, then live | Next |
| 6 | Backfill job | |
| 7 | Profiles project for the milestones, and audiences to the marketing tool | Owner to decide |

## What changed from the original plan

The original plan computed milestones in the provider with `Once`,
`Threshold`, `Inactivity` and `Frequency` triggers, NATS KV counters, a
sweeper and a job that seeded provider state. That moved to Profiles because:

- Counting in the provider had to survive redelivery and needed per-subject
  state that is hard to correct once wrong. Profiles recomputes from history.
- Milestone thresholds are product decisions that change; they no longer need
  a provider release.
- The seed job becomes a simpler backfill of plain events.

The cost is latency: Profiles runs on a schedule (hourly or daily), which is
acceptable for lifecycle and onboarding use.

## Open items

| Item | Status |
|---|---|
| RudderStack workspace | Confirm whether one exists and which destinations it has |
| Profiles | Confirm it is on the RudderStack plan; otherwise compute milestones in the marketing tool |
| Marketing tool | Which destination receives traits and audiences (HubSpot, Customer.io, …) |
| BigQuery | Only the `billing-export` dataset exists today, unrelated to analytics; `product_analytics` is new |
| Payment information completed | Billing resource to confirm |
| Desktop app downloaded | Not visible to the control plane; track on the download page |
| DNS zone, load balancer, tunnel kinds | Resource groups/names to confirm |
| Frequent / active datumctl user | Definition needed (lives in Profiles) |
| Galactic usage | Definition needed |
