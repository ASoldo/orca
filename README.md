# orca

`orca` is a modern Kubernetes terminal cockpit inspired by `k9s`, rebuilt in Rust with `ratatui`.

## Highlights

- Fast async runtime (`tokio`) and typed Kubernetes client (`kube` + `k8s-openapi`)
- Responsive layout for wide, medium, and small terminal sizes
- Vim-style navigation and command workflow
- Powerline-inspired top/status bars for dense terminal context
- Compact icon-based top bar with active resource context
- Dashboard side pane (gauges + health map) tied to current selection
- Multi-resource exploration:
  - Workloads: CronJobs, DaemonSets, Deployments, Jobs, Pods, ReplicaSets, ReplicationControllers, StatefulSets
  - Service: Ingresses, IngressClasses, Services
  - Config & Storage: ConfigMaps, PersistentVolumeClaims, Secrets, StorageClasses, PersistentVolumes
  - Cluster & RBAC: ClusterRoleBindings, ClusterRoles, Events, Namespaces, NetworkPolicies, Nodes, RoleBindings, Roles, ServiceAccounts
  - Custom Resource Definitions catalog (`CRD`)
- Full short/long aliases in `:` / `>` commands (kubectl-style): `po|pods`, `cj|cronjobs`, `ds|daemonsets`, `rs`, `rc`, `ing`, `cm`, `pvc`, `sa`, `rb`, `crb`, `np`, `sc`, `pv`, `ns`, `crd`, ...
- Watch streams for all built-in mapped resources
- YAML detail mode + compact dashboard overview mode
- Dashboard widgets: status bars, readiness/stability gauges, load/risk trends
- On-demand YAML detail pane for selected resources (`Enter`/`d`, `Esc` to return)
- Pod log loading (`l` or `:logs`) in the details pane
- Namespace scope switching (`--namespace`, `--all-namespaces`, `:ns`, `:all-ns`)
- Context and cluster switching (`:ctx`, `:cluster`, `> ctx`, `> cluster`)
- Watch-driven live refresh for active resources (with polling fallback)
- DevOps actions:
  - `:delete` (with confirmation)
  - `:restart` (Deployments/StatefulSets, with confirmation)
  - `:scale <replicas>` (Deployments/StatefulSets, with confirmation)
  - `:exec <cmd...>` (selected pod)
  - `:shell` / `:ssh` / `:bash` (interactive shell in selected pod)
  - `:edit` (opens selected resource in your editor)
  - `:port-forward <local>:<remote>` (selected pod/service)
  - Live port-forward indicator in Pods/Services (`PF` column + top badge on selected row)

## Run

```bash
cargo run -- --all-namespaces
```

or

```bash
cargo run -- --namespace default --refresh-ms 1500
```

`kubectl` must be available in `PATH` for `:exec`, `:shell`, `:edit`, and `:port-forward`.
`$KUBE_EDITOR` is respected for `:edit`; if unset, `orca` forwards `$EDITOR` to `kubectl`.

## Keybindings

- `Left` / `Right`: previous/next resource tab
- `j` / `k`: move selection
- `gg` / `G`: first/last row
- `Ctrl+u` / `Ctrl+d`: jump up/down
- `/`: filter mode
- `:`: command mode
- `>`: jump mode (quick tab/resource jump)
- `Tab`: autocomplete in `:` and `>` modes
- `Up`/`Down` or `Ctrl+p`/`Ctrl+n`: cycle autocomplete suggestions
- `r`: refresh active tab
- `l`: load logs for selected pod
- `s`: open shell in selected pod
- `e`: edit selected resource
- `p`: open `:port-forward ` prompt
- `Enter` or `d`: open selected resource details
- `Esc`: return from details pane to dashboard
- `Tab` (normal mode): toggle table/detail focus
- `y` / `n`: confirm/cancel pending actions
- `?`: toggle help
- `q`: quit

## Commands

- `:q`, `:quit`, `:exit`
- `:refresh`
- `:ctx <context-name>`
- `:cluster <cluster-name|cluster-server>`
- `:ns` (switch to Namespaces tab)
- `:ns <namespace>`
- `:namespace` / `:namespaces` (same behavior as `:ns`)
- `:all-ns`
- `:<resource>` (direct tab switch using short/long names: `po`, `pods`, `deployments`, `svc`, `events`, `ns`, `crd`, ...)
- `:<resource> <query>` (switch tab and apply filter)
- `:tab <pods|deployments|statefulsets|jobs|services|nodes|events|namespaces>`
- `:tab <pods|deployments|statefulsets|jobs|services|nodes|events|namespaces|crd>`
- `:filter <query>`
- `:clear`
- `:logs`
- `:edit`
- `:delete`
- `:restart`
- `:scale <replicas>`
- `:exec <command...>`
- `:shell [container] [shell]`
- `:ssh [container] [shell]`
- `:bash`
- `:port-forward <local>:<remote>`
- `:crd <name|kind|plural>`
- `:crd-refresh`
- `:help`

## Architecture

- `src/main.rs`: app runtime, event loop, refresh scheduling
- `src/app.rs`: state machine, commands, interaction model
- `src/input.rs`: key mapping and mode-specific actions
- `src/k8s.rs`: Kubernetes data fetchers and pod logs API
- `src/ui.rs`: responsive `ratatui` rendering
- `src/model.rs`: shared domain models
- `src/cli.rs`: CLI arguments
