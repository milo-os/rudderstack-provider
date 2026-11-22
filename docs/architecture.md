# RudderStack Provider Architecture

## Overview

The RudderStack provider turns platform activity into product analytics. It
consumes Kubernetes audit events from NATS JetStream, evaluates them against
`TrackingRule` resources, and sends `track`, `identify` and `group` calls to
RudderStack.

The provider forwards events and keeps almost no state. Milestones such as
"first domain", "more than 3 domains" or "inactive for 14 days" are not
computed here. RudderStack loads every event into BigQuery, and scheduled
queries there compute the milestones from the full history. The results go to
marketing tools as user and organization traits.

It follows the design of the [search] resource indexer: a durable JetStream
consumer, policy resources with CEL conditions compiled once and cached, and
batches that are acknowledged only after the backend accepts them.

[search]: https://github.com/milo-os/search/blob/main/docs/components/resource-indexer.md

```
AUDIT_EVENTS ─▶ rudderstack-provider ─▶ RudderStack ─▶ BigQuery (product_analytics)
 (NATS)          match rules, send                       │
                 raw events                              ▼
                                            BigQuery scheduled queries
                                         (milestones, counts, inactivity)
                                                         │
                                                         ▼
                                  milestone tables ─▶ reverse ETL ─▶ marketing tool
```

## Responsibilities

| Concern | Owner |
|---|---|
| Which platform actions become events, and their properties | `TrackingRule` resources (this repo) |
| Reliable delivery, deduplication of redelivered events | Provider |
| Daily activity signals (`Active`, `datumctl Used`) without flooding RudderStack | Provider (once-per-day limit) |
| Event storage and history | BigQuery, loaded by RudderStack |
| "First X", "more than N", "inactive for N days", "frequent user" | BigQuery scheduled queries |
| Sending milestones to marketing tools | RudderStack reverse ETL from the milestone tables |

This split keeps milestone definitions out of the provider. Changing "more
than 3 domains" to "more than 5" is a SQL change, not a provider release. The
queries also recompute from the complete history, which rules out drift from
missed or duplicated events.

## Why audit events, not activities

Activities are the human-readable output of the activity processor, but an
activity produced from an audit log does not record the verb. Without it the
provider cannot tell "domain added" from "domain removed", and the removal
events are what let the milestone queries count resources that currently
exist. Audit
events also carry the `userAgent` (needed for datumctl signals), the
authenticated user, the request scope and the object.

## TrackingRule

`TrackingRule` (`analytics.miloapis.com/v1alpha1`, cluster-scoped) maps audit
events to one RudderStack call. Rules are added through GitOps without a code
change. See [`config/rules`](../config/rules).

| Field | Purpose |
|---|---|
| `event` | RudderStack event name, Object + Action (`Domain Added`) |
| `call` | `Track` (default), `Identify` or `Group` |
| `match.resource` | `{group, resource}` as audit events name it (plural resource) |
| `match.verbs` | Audit verbs to match |
| `match.conditions` | CEL, OR semantics |
| `subject` | `User` (default) or `Organization` |
| `userId` | CEL override for the RudderStack userId |
| `groupId` | CEL override for the RudderStack groupId; defaults to the request's organization, as does an empty result |
| `limit` | Optional `OncePerDay`: send at most one call per subject per UTC day |
| `properties` | CEL values sent with the call; nothing else leaves the platform |

CEL expressions can read `verb`, `user`, `userAgent`, `objectRef`, `object`,
`tenant` and `actor`. The controller compiles every expression and sets the
rule's `Ready` condition; rules that are not Ready are not evaluated.

`actor.kind` says who made the request, decided in one place so every rule
agrees:

- `system`: a `system:` username, meaning a platform controller or
  Kubernetes service account.
- `serviceaccount`: a customer machine account, such as a CI pipeline. These
  authenticate as `{name}@{project}.{suffix}`, for example
  `ci-a1b2c3@my-project.iam.datumapis.com`, matched by
  `--service-account-email-suffix` (`iam.datumapis.com` in staging and
  production). Without the flag they count as users.
- `user`: a person.

### Event catalog

Every "added" event has a matching "removed" event so the milestone queries
can compute current counts. These rules match every actor, not only humans: resources are
often removed by the system (deleting a project makes the garbage collector
delete its resources), and a missed removal would leave a count too high for
good. Each event carries:

- `name`, `namespace` and `project`, which identify the resource, so the
  queries match a removal to its add regardless of who made either.
- `changeSource`: `human`, `automation` (a customer machine account) or
  `system` (the platform), from `actor.kind`, so milestones such as "user
  removes domains" can count only human actions.

| Event | Source | Feeds |
|---|---|---|
| `Signed Up` + `identify` (email) | `iam.miloapis.com/users` create | Sign up |
| `Project Created` / `Project Deleted` | `resourcemanager.miloapis.com/projects` | First project |
| `Domain Added` / `Domain Removed` | `networking.datumapis.com/domains` | First, >3 (user), >10 (org), removes domains |
| `Proxy Added` / `Proxy Removed` | `networking.datumapis.com/httpproxies` | First proxy, ALB added (the console's "ALB" is an HTTPProxy) |
| `Workload Deployed` / `Workload Removed` | `compute.datumapis.com/workloads` | Deployed compute, >10 workloads, spins down compute |
| `Contributor Added` / `Contributor Removed` | `resourcemanager.miloapis.com/organizationmemberships` | Add contributor, >10 teammates |
| `DNS Zone Added` / `DNS Zone Removed` | `dns.networking.miloapis.com/dnszones` | First zone, >5, >15 org, removes zones |
| `Network Load Balancer Added` / `Network Load Balancer Removed` | `networking.datumapis.com/networkloadbalancers` | Layer-4 load balancer adoption |
| `Tunnel Added` / `Tunnel Removed` | `networking.datumapis.com/connectors` | >4 active tunnels |
| `DNS Record Set Added` / `DNS Record Set Removed` | `dns.networking.miloapis.com/dnsrecordsets` | Zones in real use (with records) |
| `Service Account Created` / `Service Account Deleted` | `iam.miloapis.com/serviceaccounts` | Automation adoption |
| `Service Account Key Created` / `Service Account Key Deleted` | `identity.miloapis.com/serviceaccountkeys` | Automation wired up (a pipeline credential exists) |
| `datumctl Used` (once per day) | Any request by a person (`actor.kind == "user"`) with a `datumctl/` user agent | First datumctl access, frequent / active datumctl user |
| `Active` (once per day) | Any request by a person (`actor.kind == "user"`) | Inactivity > 14 days |
| `group` (`name`, `displayName`) | `resourcemanager.miloapis.com/organizations` create/update | Org-level milestones |

## Milestones

Milestones are computed in BigQuery by scheduled queries over the event
tables RudderStack loads. RudderStack's BigQuery destination writes one table
per event (`domain_added`, `domain_removed`, …) plus `tracks`, `identifies`,
`groups` and `users`, in the `product_analytics` dataset.

### How they run

- Each milestone table has one query, kept as a `.sql` file in git and
  scheduled through the BigQuery Data Transfer Service. The schedules are
  Pulumi resources in `datum-cloud/infra` (`infra/gcp`), next to the dataset.
- Queries run hourly and rebuild their table in full (`WRITE_TRUNCATE`).
  Recomputing from history means a fixed definition corrects past results,
  and a missed or duplicated event never leaves a count wrong for good. At
  this volume a full rebuild costs little.
- RudderStack's warehouse loads can contain the same call more than once.
  Every query deduplicates on the event's `id` column, which is the
  `messageId`, so the provider's deterministic messageIds make duplicates
  harmless.

### Output tables

| Table | Key | Columns (illustrative) |
|---|---|---|
| `user_milestones` | `user_id` | `signed_up_at`, `first_project_at`, `first_domain_at`, `active_domains`, `domains_over_3_at`, `first_proxy_at`, `first_workload_at`, `first_datumctl_at`, `datumctl_days_14d`, `last_active_at` |
| `org_milestones` | `group_id` | `active_domains`, `domains_over_10_at`, `active_zones`, `active_workloads`, `workloads_over_10_at`, `contributors`, `contributors_over_10_at` |

A milestone is a timestamp column that is null until it is reached, so a
marketing tool can trigger on it becoming set.

### Definitions

Illustrative; the SQL in git is authoritative.

| Milestone | Definition over BigQuery events |
|---|---|
| First domain | `min(timestamp)` of `Domain Added` per user |
| > 3 domains (user) | Domains the user added, keyed by `project`/`name`, with no later `Domain Removed`, > 3 |
| > 10 domains (org) | Same per `context_group_id` > 10 |
| > 4 active tunnels | Tunnels added with no later `Tunnel Removed` > 4 |
| Removes domains | `Domain Removed` with `change_source = 'human'` |
| Frequent datumctl user | Distinct days with `datumctl Used` in the last 14 ≥ N |
| Inactive > 14 days | `max(timestamp)` of `Active` older than 14 days |

### Delivery to marketing tools

RudderStack reverse ETL reads `user_milestones` and `org_milestones` and sends
their rows as `identify` and `group` traits to the marketing tool. If the plan
does not include reverse ETL, the marketing tool can read the tables from
BigQuery directly instead.

### Why not RudderStack Profiles

Profiles computes the same kind of features inside RudderStack, but it is not
on the plan. It is also not needed: Profiles' main advantage is identity
stitching, linking anonymous visitors to the users they become, and every
event here already carries a known Milo user ID. Revisit it if anonymous web
or app tracking is added, such as on the desktop download page, and
pre-sign-up activity needs to be tied to accounts.

## Delivery guarantees

- Messages are acknowledged only after RudderStack returns 2xx for every call
  they produced. Network errors, `429` and `5xx` nack the message with a 10s
  delay.
- Every call has a deterministic `messageId`, so RudderStack drops duplicates
  from redelivery: `<auditID>/<rule>`, or `<rule>/<subject hash>/<date>` for
  once-per-day rules.
- A batch RudderStack rejects as malformed (`400`, `413`) is split in half
  repeatedly until the rejected calls are isolated. Those are dropped and
  counted.
- Every other failure is retried, including `401`, `403` and `404`. Those
  mean a wrong write key or data plane URL, so a configuration mistake shows
  up as a growing backlog rather than as lost events.
- Each request stamps `sentAt`, so RudderStack keeps each event's
  `originalTimestamp` even when it is delivered late, after an outage or in a
  backfill.
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

Each replica remembers the keys it has seen taken, so most repeat checks of
the day never reach the bucket. Without it every matching request would wait
on a round trip, and a slow bucket would stall the consumer. Errors are not
remembered, so the next check retries the bucket.

Every failure mode sends rather than drops, and relies on the call's
`<rule>/<subject hash>/<date>` `messageId` for RudderStack to deduplicate:

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
  organization need, since that request is not made inside it. Project
  events read it from the project's owner, because the console deletes
  projects through the platform API, outside any organization. An expression
  that returns an empty string falls back to the request's organization.
- `track` and `identify` calls carry the organization as `context.groupId`;
  `group` calls carry it as `groupId` with the rule's properties as traits.
- A project the provider has not seen yet (created moments before) leaves
  `groupId` empty and is counted in `rudderstack_provider_orgs_unresolved_total`.
  The event is still sent: the milestone queries can recover the organization
  from the project's `Project Created` event.
- Rules with an `Organization` subject, and `group` calls, are skipped when
  there is no organization.

## Backfill

The milestone counts need the resources customers created before launch. A
one-off job lists existing resources and sends their "added" events with the
resource's `creationTimestamp` as `originalTimestamp` and `backfill/<uid>` as
`messageId`, plus `identify` and `group` for existing users and
organizations. It runs once, before milestones are switched on for marketing.

## Deployment

The image and a kustomize bundle are published to `ghcr.io/milo-os` on every
push. `datum-cloud/infra` deploys from the bundle, the same way it deploys
other milo-os providers:

| Bundle path | Contents | Applied to |
|---|---|---|
| `base/crd` | `TrackingRule` CRD | Milo |
| `rules` | The event catalog's `TrackingRule`s | Milo |
| `base/manager` | Manager Deployment, ServiceAccount, metrics Service | Core control plane cluster |
| `components/controller_rbac` | Generated ClusterRole: read rules and projects, write rule status | Milo, with `base/crd` |
| `components/milo-integration` | Points the manager at Milo for rules, projects and leader election; disables the unused ServiceAccount token | With the manager |
| `components/observability` | ServiceMonitor | With the manager |
| `components/nats-config` | `AUDIT_EVENTS` consumer and the limits KV bucket, applied by NACK | Core control plane cluster |

`base/rbac` and `components/namespace` are for running outside Milo, such as
`make deploy` into a development cluster.

The manager's settings are env vars the deployment overlay can patch.
`DRY_RUN` defaults to `true`, and with no `NATS_URL` only the controller
runs, so an overlay opts in to consuming and then to sending. In dry-run the
provider logs each call's type, event, IDs, property and trait names and size,
never property or trait values, which can hold email addresses. The `userId`
is logged only as a hash, because it can be an email address. The overlay
supplies:

- the Milo kubeconfig, client certificate and CA bundle (see
  `milo-integration`). The certificate needs only `controller_rbac` bound to
  its user, plus a Role for its Lease in `milo-system`. It should not carry
  `system:masters`: the pod holds an external write key and calls out to
  RudderStack.
- `NATS_URL` and the NATS client certificate (`NATS_TLS_*`)
- `SERVICE_ACCOUNT_EMAIL_SUFFIX`, so customer machine accounts are not
  counted as people
- `RUDDERSTACK_DATA_PLANE_URL` (must be `https://`), `DRY_RUN=false`, and a
  Secret named `rudderstack-provider-write-key` with key `writeKey`

Metrics are served over plain HTTP. They hold only counters, and the
authenticated alternative would send scrapers' tokens to Milo, which cannot
verify them. No webhook server runs and no certificate is generated.

## Rollout

- The JetStream consumer starts with `deliverPolicy: new` so history is not
  replayed into analytics.
- `--dry-run` logs each call's shape instead of sending it; staging runs this
  way first.

## Phases

| # | Scope | Status |
|---|---|---|
| 1 | Scaffold, `TrackingRule` CRD, CEL compilation and Ready condition | Done (#4) |
| 2 | Audit consumer, RudderStack sink, dry-run | Done (#4) |
| 3 | Replace triggers with the optional `limit`; add the "removed" rules; the event catalog | Done (#4) |
| 4 | Once-per-day limit (NATS KV), which turns on `datumctl Used` and `Active`; organization resolution and `group` calls | Done (#5) |
| 5a | Deployable bundle in the milo-os provider layout | Done (#6) |
| 5b | Staging in dry-run: Milo CRD, rules and least-privilege RBAC; manager, NACK consumer and KV bucket | In review (datum-cloud/infra#9648) |
| 5c | BigQuery `product_analytics` dataset, RudderStack loader service account and GCS staging bucket (Pulumi, `infra/gcp`) | Next |
| 5d | Write key ExternalSecret, `DRY_RUN=false`; production overlay | |
| 6 | Backfill job | |
| 7 | Milestone scheduled queries and their Pulumi schedules (`infra/gcp`); reverse ETL to the marketing tool | |

## What changed from the original plan

The original plan computed milestones in the provider with `Once`,
`Threshold`, `Inactivity` and `Frequency` triggers, NATS KV counters, a
sweeper and a job that seeded provider state. That moved downstream to
BigQuery because:

- Counting in the provider had to survive redelivery and needed per-subject
  state that is hard to correct once wrong. The queries recompute from history.
- Milestone thresholds are product decisions that change; they no longer need
  a provider release.
- The seed job becomes a simpler backfill of plain events.

The cost is latency: the queries run hourly, which is acceptable for lifecycle
and onboarding use. RudderStack Profiles was considered for this and not
chosen; see [Why not RudderStack Profiles](#why-not-rudderstack-profiles).

## Open items

| Item | Status |
|---|---|
| RudderStack plan | On a free trial; moving to Growth before sending is turned on (5d). Size it from staging dry-run volumes |
| Reverse ETL | Confirm it is on the Growth plan; otherwise the marketing tool reads BigQuery directly |
| Marketing tool | Which destination receives the milestone traits (HubSpot, Customer.io, …) |
| BigQuery | Only the `billing-export` dataset exists today, unrelated to analytics; `product_analytics` is new |
| Payment information completed | Billing resource to confirm |
| Desktop app downloaded | Not visible to the control plane; track on the download page |
| Frequent / active datumctl user | Definition needed (lives in the milestone SQL) |
| Galactic usage | Definition needed |
