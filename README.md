# orca

`orca` is a Kubernetes terminal cockpit inspired by `k9s`, built in Rust with `ratatui`.

## Current capabilities

- Async TUI runtime with `tokio` + typed Kubernetes client (`kube`, `k8s-openapi`)
- Powerline-style header/footer with compact context, scope, and status data
- Multi-view workflow (view slots) for fast context switching without losing state
- Vim-style navigation and command/jump/filter modes
- Single main pane that can show:
  - resource table
  - dashboard overview (`o`)
  - syntax-highlighted details (`d`)
  - logs/output overlay
  - pod container picker
- Watch-based refresh for mapped resources, with periodic refresh fallback
- Context, cluster, and user switching from kubeconfig
- Namespace scoping (`--namespace`, `--all-namespaces`, `:ns`, `:all-ns`)
- Pod/service port-forward management with live PF indicators
- YAML/JSON syntax highlighting in details view

## Supported resources

- Pods
- CronJobs
- DaemonSets
- Deployments
- ReplicaSets
- ReplicationControllers
- StatefulSets
- Jobs
- Services
- Ingresses
- IngressClasses
- ConfigMaps
- PersistentVolumeClaims
- Secrets
- StorageClasses
- PersistentVolumes
- ServiceAccounts
- Roles
- RoleBindings
- ClusterRoles
- ClusterRoleBindings
- NetworkPolicies
- Nodes
- Events
- Namespaces
- CRD (custom resources + CRD catalog)

## Requirements

- Access to a Kubernetes cluster (`$KUBECONFIG` or in-cluster config)
- `kubectl` in `PATH` for subprocess actions:
  - `:exec`
  - `:shell` / `:ssh` / `:bash`
  - `:edit`
  - `:port-forward`
- Optional but recommended: `metrics-server` for richer CPU/RAM dashboard data

`orca` uses `$KUBE_EDITOR` for `:edit`; if unset, it forwards `$EDITOR` to `kubectl`.

## Run

```bash
cargo run --release -- --all-namespaces
```

```bash
cargo run --release -- --namespace default --refresh-ms 1500
```

## CLI flags

- `--refresh-ms <ms>`: refresh interval in milliseconds (minimum enforced at runtime: `500`)
- `-n, --namespace <name>`: start in a specific namespace
- `-A, --all-namespaces`: start with all namespaces
- `--log-filter <level>`: tracing filter (default: `info`)

## Interaction model

- `Enter` is drill-down, not details:
  - `Namespaces -> Pods` (sets namespace scope)
  - `Pods -> Containers` (container picker)
  - `Deployments/DaemonSets/StatefulSets/ReplicaSets/ReplicationControllers/Jobs/CronJobs -> Pods`
  - `Services -> Pods`
- `d` opens details mode for the selected row
- `Esc` goes back one step (logs -> containers -> previous flow/root)
- `o` toggles overview dashboard in the main pane

## Keybindings

- `Left` / `Right`: previous/next resource tab
- `j` / `k`, `Up` / `Down`: move selection
- `gg` / `G`: top / bottom
- `Ctrl+u` / `Ctrl+d`, `PageUp` / `PageDown`: page scroll
- `/`: filter mode
- `:`: command mode
- `>`: jump mode
- `Tab` (input modes): autocomplete
- `Up` / `Down` or `Ctrl+p` / `Ctrl+n` (input modes): autocomplete selection
- `Enter` (or terminal fallbacks `Ctrl+m` / `Ctrl+j` in input mode): submit input
- `l`: logs for selected pod/container
- `Shift+L`: previous/related logs (workload/service aware)
- `s`: open shell (`/bin/sh`) in selected pod
- `e`: edit selected resource
- `p`: prefill `:port-forward ` command
- `d`: open details view
- `o`: open/close overview
- `Tab` (normal mode): toggle focus (`table`/`details` when details mode is active)
- `y` / `n`: confirm or cancel pending actions
- `?`: help modal
- `q`: quit

### View slots

- `Ctrl+0..9` switches/creates view slots
- `0..9` also switches view slots in normal mode
- `Alt+0..9` switches view slots in input modes
- View state is preserved per slot (tab, scope, filter, overlays, selection)

## Command mode (`:`)

Supported commands:

- `:q`, `:quit`, `:exit`
- `:refresh` (`:reload`, `:r`)
- `:ctx <context>` (`:context`, `:use-context`)
- `:cluster <cluster-name-or-server>` (`:cl`)
- `:user <user>` (`:usr`)
- `:contexts`, `:clusters`, `:users`
- `:all-ns` (`:all`, `:allns`, `:all-namespaces`)
- `:ns` / `:namespace` / `:namespaces`
- `:ns <namespace>`
- `:<resource>` (switch tab by alias)
- `:<resource> <filter>`
- `:<resource> <namespace>/<name>`
- `:filter <query>`
- `:clear`
- `:logs`
- `:edit` (`:e`)
- `:delete` (`:del`) (confirmation required)
- `:restart` (Deployments/StatefulSets, confirmation required)
- `:scale <replicas>` (Deployments/StatefulSets, immediate)
- `:exec <command...>` (Pods tab)
- `:shell [container] [shell]`
- `:ssh [container] [shell]`
- `:bash`
- `:pf <local>:<remote>` (`:port-forward`)
- `:crd <name|kind|plural>` (`:custom`)
- `:crd-refresh`
- `:help`

Compatibility command:

- `:tab <resource> [filter-or-target]`

## Jump mode (`>`)

- Supports the same context/cluster/user and resource aliases for fast navigation
- Supports namespaced targets (for example `>po my-ns/my-pod`)
- Supports fuzzy jump by resource name/namespace when no explicit alias is provided
- Resets to the current flow root before executing jump selection

## Resource aliases

Short aliases accepted in `:` and `>` include:

- `po`, `cj`, `ds`, `deploy`, `rs`, `rc`, `sts`, `job`
- `svc`, `ing`, `ingclass`, `cm`, `pvc`, `secret`, `sc`, `pv`
- `sa`, `role`, `rb`, `crole`, `crb`, `np`, `node`, `event`, `ns`, `crd`

Long names (`pods`, `deployments`, `services`, etc.) are also supported.

## DevOps actions behavior

- `:delete` and `:restart` are guarded by confirmation (`y/n`, `Enter` also confirms)
- `:scale` executes immediately and refreshes the active resource table
- `l`/`:logs` are pod/container log focused
- `Shift+L` resolves related pod logs for workload/service resources
- Port-forward sessions are tracked and shown in:
  - `PF` table column for Pods/Services
  - header badge for selected resource

## Project layout

- `src/main.rs`: runtime loop, event handling, refresh/watch orchestration
- `src/app.rs`: state machine, mode handling, command parser, drill-down flow
- `src/input.rs`: key mapping by mode
- `src/k8s.rs`: Kubernetes API gateway, table builders, actions, metrics, discovery
- `src/ui.rs`: `ratatui` rendering, powerline bars, dashboard, syntax highlighting
- `src/model.rs`: shared tab/data models
- `src/cli.rs`: CLI argument definitions
