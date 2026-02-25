<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/5cf48dd3-55aa-46f4-bb01-eca2730632f7" />

# orca

<img width="953" height="515" alt="image" src="https://github.com/user-attachments/assets/5b5bc695-36fd-4168-bcad-eaca5f921c08" />


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
  - embedded pod shell overlay (`s`, `:shell`, `:ssh`, `:bash`)
  - pod container picker
- Watch-based refresh for mapped resources, with periodic refresh fallback
- Context, cluster, and user switching from kubeconfig
- Context/user/cluster catalog overlays (`:ctx`, `:usr`, `:cluster` without args)
- Namespace scoping (`--namespace`, `--all-namespaces`, `:ns`, `:all-ns`)
- Pod/service port-forward management with live PF indicators
- YAML/JSON syntax highlighting in details view
- DevOps tool overlays for Argo CD, Helm, Terraform, Ansible, Docker, OpenShift, and Kustomize
- Fleet pulse snapshot (`:pulses`) and resource relationship trace (`:xray`)
- Read-only safety mode (`:readonly on|off|toggle`, `ORCA_READONLY=1`)
- Runtime aliases/plugins/hotkeys from YAML config with automatic reload

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
- Optional DevOps toolchain for overlays:
  - `argocd`, `helm`, `terraform`, `ansible-playbook`, `docker`, `oc`, `kustomize`

`orca` uses `$KUBE_EDITOR` for `:edit`; if unset, it forwards `$EDITOR` to `kubectl`.
Set `ORCA_READONLY=1` to start in safety mode where mutating actions are blocked.
Set `ORCA_CONFIG=/path/to/orca.yaml` to pin a specific runtime config file.

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
- `Esc` goes back one step (shell/logs -> containers -> previous flow/root)
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
- `s`: open embedded shell (`/bin/sh`) in selected pod (inside ORCA)
- `e`: edit selected resource
- `p`: prefill `:port-forward ` command
- `d`: open details view
- `o`: open/close overview
- `Tab` (normal mode): toggle focus (`table`/`details` when details mode is active)
- `y` / `n`: confirm or cancel pending actions
- `?`: help modal
- `q`: quit

### View slots

- `Ctrl+1..9` switches/creates view slots
- `1..9` also switches view slots in normal mode
- `Ctrl+Shift+1..9` switches view slots in input modes
- `Ctrl+Alt+0..9` deletes a view slot
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
- `:ops`, `:tools`
- `:readonly on|off|toggle` (`:ro`)
- `:config` (shows loaded config source, aliases, plugins, hotkeys)
- `:alerts` (`:alert`) high-signal incident snapshot
- `:pulses` (`:pulse`)
- `:xray` (`:xr`, `:x`) on selected row (or explicit target)
- `:argocd [app-name]`
- `:helm [release]`
- `:tf` (`:terraform`)
- `:ansible` (`:ans`)
- `:docker`
- `:rbac [subject]` (uses `kubectl auth can-i --list`, optional `--as`)
- `:oc` (`:openshift`)
- `:kustomize [path]`
- `:plugin <name> [args...]` (`:plug`) runs configured plugin command

Compatibility command:

- `:tab <resource> [filter-or-target]`

## Jump mode (`>`)

- Supports the same context/cluster/user and resource aliases for fast navigation
- Supports DevOps overlays (`>tools`, `>argocd`, `>helm`, `>tf`, `>ansible`, `>docker`, `>rbac`, `>oc`, `>kustomize`)
- Supports observability overlays (`>pulses`, `>xray`)
- Supports incident overlays (`>alerts`)
- Supports config/plugin actions (`>config`, `>plugin <name> ...`)
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
- `s` / `:shell` / `:ssh` / `:bash` open an embedded shell overlay (`sh` pane label); `Esc` closes it
- Port-forward sessions are tracked and shown in:
  - `PF` table column for Pods/Services
  - header badge for selected resource

## Runtime config (`orca.yaml`)

ORCA can load aliases and plugins from:
- `ORCA_CONFIG` (if set)
- `./orca.yaml`, `./orca.yml`, `./.orca.yaml`
- `$HOME/.config/orca/config.yaml`

Example:

```yaml
aliases:
  d: "deployments"
  sys: "ns kube-system"

plugins:
  - name: "describe-pod"
    command: "kubectl"
    args: ["describe", "pod", "{name}", "-n", "{namespace}"]
    description: "Describe currently selected pod"
    mutating: false

hotkeys:
  - key: "ctrl+shift+p"
    command: "pulses"
    description: "Open fleet pulses"
  - key: "ctrl+shift+x"
    command: "xray"
    description: "Open xray for selected row"
  - key: "ctrl+shift+g"
    command: "po kube-system/coredns"
    jump: true
    description: "Jump directly to coredns pod"
```

Supported placeholders in plugin args:
- `{name}`, `{namespace}`, `{target}`, `{resource}`
- `{context}`, `{cluster}`, `{user}`, `{scope}`
- `{all_namespaces}`, `{args}`
- `{extra}` to splice all user-supplied plugin args

## Project layout

- `src/main.rs`: runtime loop, event handling, refresh/watch orchestration
- `src/app.rs`: state machine, mode handling, command parser, drill-down flow
- `src/input.rs`: key mapping by mode
- `src/k8s.rs`: Kubernetes API gateway, table builders, actions, metrics, discovery
- `src/config.rs`: runtime YAML config loader/watcher (aliases + plugins)
- `src/ui.rs`: `ratatui` rendering, powerline bars, dashboard, syntax highlighting
- `src/model.rs`: shared tab/data models
- `src/cli.rs`: CLI argument definitions
