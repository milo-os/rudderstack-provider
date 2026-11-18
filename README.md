# RudderStack Provider

Sends product analytics to [RudderStack] from Milo platform activity. The
provider consumes Kubernetes audit events from NATS JetStream, evaluates them
against `TrackingRule` resources, and sends `track`, `identify` and `group`
calls. It shares its design with [search].

See [docs/architecture.md](docs/architecture.md) for the design.

[RudderStack]: https://www.rudderstack.com
[search]: https://github.com/milo-os/search

## TrackingRule

```yaml
apiVersion: analytics.miloapis.com/v1alpha1
kind: TrackingRule
metadata:
  name: domains-added-user-over-3
spec:
  event: Domains Added Over 3
  match:
    resource:
      group: networking.datumapis.com
      resource: domains
    verbs: [create, delete]
  trigger:
    type: Threshold
    threshold:
      atLeast: 4
```

The rules for the analytics event list are in [config/samples](config/samples).

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
