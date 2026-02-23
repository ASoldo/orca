mod app;
mod cli;
mod input;
mod k8s;
mod model;
mod ui;

use anyhow::{Context, Result};
use app::{App, AppCommand};
use clap::Parser;
use cli::CliArgs;
use crossterm::event::{
    Event, EventStream, KeyEventKind, KeyboardEnhancementFlags, PopKeyboardEnhancementFlags,
    PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    supports_keyboard_enhancement,
};
use futures::{StreamExt, TryStreamExt};
use k8s::KubeGateway;
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event as KubeEvent, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    ReplicationController, Secret, Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass, NetworkPolicy};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};
use k8s_openapi::api::storage::v1::StorageClass;
use kube::runtime::watcher::{Config as WatchConfig, watcher};
use kube::{Api, Client};
use model::{NamespaceScope, ResourceTab};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use std::collections::HashMap;
use std::io::{self, Stdout};
use std::process::Stdio;
use std::time::Instant;
use tokio::process::Command as TokioCommand;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Duration, MissedTickBehavior, interval, timeout};
use tracing::{debug, warn};
use tracing_subscriber::EnvFilter;

type TuiTerminal = Terminal<CrosstermBackend<Stdout>>;
const TABLE_REFRESH_TIMEOUT: Duration = Duration::from_secs(4);
const METRICS_REFRESH_TIMEOUT: Duration = Duration::from_secs(2);
const CRD_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

enum LoopEffect {
    None,
    RestartWatchers,
}

#[derive(Debug, Clone)]
struct PortForwardExitEvent {
    pid: u32,
    tab: ResourceTab,
    namespace: String,
    name: String,
    local_port: u16,
    remote_port: u16,
    result: std::result::Result<std::process::ExitStatus, String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    init_tracing(&args.log_filter)?;

    let mut gateway = KubeGateway::new().await?;
    let namespace_scope = resolve_namespace_scope(&args, &gateway);

    let mut app = App::new(
        gateway.cluster().to_string(),
        gateway.context().to_string(),
        namespace_scope,
    );
    app.set_user(gateway.user().to_string());
    app.set_kube_catalog(
        gateway.available_contexts(),
        gateway.available_clusters(),
        gateway.available_users(),
    );

    if args.all_namespaces && args.namespace.is_some() {
        warn!("both --all-namespaces and --namespace were provided, using all namespaces");
    }

    run(&mut app, &mut gateway, args.refresh_ms.max(500)).await
}

fn init_tracing(level_filter: &str) -> Result<()> {
    let filter = EnvFilter::try_new(level_filter)
        .or_else(|_| EnvFilter::try_new("info"))
        .context("failed to initialize tracing filter")?;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .with_writer(std::io::sink)
        .try_init();

    Ok(())
}

fn resolve_namespace_scope(args: &CliArgs, gateway: &KubeGateway) -> NamespaceScope {
    if args.all_namespaces {
        NamespaceScope::All
    } else if let Some(namespace) = &args.namespace {
        NamespaceScope::Named(namespace.clone())
    } else {
        NamespaceScope::Named(gateway.default_namespace().to_string())
    }
}

async fn run(app: &mut App, gateway: &mut KubeGateway, refresh_ms: u64) -> Result<()> {
    let (mut terminal, keyboard_enhanced) = init_terminal()?;
    let run_result = run_loop(&mut terminal, app, gateway, refresh_ms).await;
    let restore_result = restore_terminal(&mut terminal, keyboard_enhanced);

    match (run_result, restore_result) {
        (Err(run_error), Err(restore_error)) => Err(anyhow::anyhow!(
            "{run_error:#}\nterminal restore error: {restore_error:#}"
        )),
        (Err(error), _) => Err(error),
        (_, Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

fn init_terminal() -> Result<(TuiTerminal, bool)> {
    enable_raw_mode().context("failed to enable raw mode")?;
    let mut stdout = io::stdout();
    let keyboard_enhanced = matches!(supports_keyboard_enhancement(), Ok(true));
    if keyboard_enhanced {
        execute!(
            stdout,
            EnterAlternateScreen,
            PushKeyboardEnhancementFlags(
                KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
                    | KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES
                    | KeyboardEnhancementFlags::REPORT_ALTERNATE_KEYS
                    | KeyboardEnhancementFlags::REPORT_EVENT_TYPES
            )
        )
        .context("failed to enter alternate screen with keyboard enhancement")?;
    } else {
        execute!(stdout, EnterAlternateScreen).context("failed to enter alternate screen")?;
    }
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("failed to create terminal backend")?;
    terminal.clear().context("failed to clear terminal")?;
    Ok((terminal, keyboard_enhanced))
}

fn restore_terminal(terminal: &mut TuiTerminal, keyboard_enhanced: bool) -> Result<()> {
    if keyboard_enhanced {
        execute!(terminal.backend_mut(), PopKeyboardEnhancementFlags)
            .context("failed to pop keyboard enhancement flags")?;
    }
    disable_raw_mode().context("failed to disable raw mode")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)
        .context("failed to leave alternate screen")?;
    terminal.show_cursor().context("failed to show cursor")?;
    Ok(())
}

async fn run_loop(
    terminal: &mut TuiTerminal,
    app: &mut App,
    gateway: &mut KubeGateway,
    refresh_ms: u64,
) -> Result<()> {
    app.set_status("Bootstrapping Kubernetes data…");
    refresh_custom_resource_catalog(app, gateway).await;
    refresh_tab(app, gateway, app.active_tab()).await;
    refresh_tab(app, gateway, ResourceTab::Namespaces).await;
    refresh_tab(app, gateway, ResourceTab::CustomResources).await;

    let mut reader = EventStream::new();
    let mut ticker = interval(Duration::from_millis(refresh_ms));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let (watch_tx, mut watch_rx) = mpsc::unbounded_channel::<ResourceTab>();
    let mut watch_tasks = start_resource_watchers(gateway.client(), watch_tx.clone());
    let mut watch_throttle = HashMap::<ResourceTab, Instant>::new();
    let (pf_tx, mut pf_rx) = mpsc::unbounded_channel::<PortForwardExitEvent>();

    loop {
        terminal
            .draw(|frame| ui::render(frame, app))
            .context("failed to render terminal frame")?;

        if !app.running() {
            break;
        }

        tokio::select! {
            maybe_event = reader.next() => {
                match maybe_event {
                    Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                        if let Some(action) = input::map_key(app.mode(), key) {
                            debug!("action={action:?}");
                            let command = app.apply_action(action);
                            terminal
                                .draw(|frame| ui::render(frame, app))
                                .context("failed to render terminal frame")?;
                            let effect =
                                execute_app_command(terminal, app, gateway, command, &pf_tx).await;
                            if matches!(effect, LoopEffect::RestartWatchers) {
                                restart_watchers(&mut watch_tasks, gateway.client(), watch_tx.clone());
                                watch_throttle.clear();
                            }
                        }
                    }
                    Some(Ok(Event::Resize(_, _))) => {}
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        app.set_status(format!("terminal event error: {error}"));
                    }
                    None => {
                        app.set_status("terminal event stream closed");
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                let active = app.active_tab();
                refresh_tab(app, gateway, active).await;
            }
            maybe_tab = watch_rx.recv() => {
                if let Some(tab) = maybe_tab
                    && should_process_watch_event(tab, &mut watch_throttle)
                    && (tab == app.active_tab() || tab == ResourceTab::Namespaces) {
                    refresh_tab(app, gateway, tab).await;
                }
            }
            maybe_event = pf_rx.recv() => {
                if let Some(event) = maybe_event {
                    let removed = app.remove_port_forward_by_pid(event.pid);
                    let target = format!(
                        "{} {}/{} {}:{}",
                        event.tab.title(),
                        event.namespace,
                        event.name,
                        event.local_port,
                        event.remote_port
                    );
                    match event.result {
                        Ok(status) if status.success() => {
                            if removed.is_some() {
                                app.set_status(format!("Port-forward closed: {target}"));
                            }
                        }
                        Ok(status) => {
                            app.set_status(format!(
                                "Port-forward exited ({status}) for {target}"
                            ));
                        }
                        Err(error) => {
                            app.set_status(format!("Port-forward failed for {target}: {error}"));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

async fn execute_app_command(
    terminal: &mut TuiTerminal,
    app: &mut App,
    gateway: &mut KubeGateway,
    command: AppCommand,
    pf_tx: &mpsc::UnboundedSender<PortForwardExitEvent>,
) -> LoopEffect {
    match command {
        AppCommand::None => {}
        AppCommand::RefreshActive => {
            let tab = app.active_tab();
            refresh_tab(app, gateway, tab).await;
        }
        AppCommand::RefreshAll => {
            let tabs = app.tabs().to_vec();
            for tab in tabs {
                refresh_tab(app, gateway, tab).await;
            }
        }
        AppCommand::RefreshCustomResourceCatalog => {
            refresh_custom_resource_catalog(app, gateway).await;
            if app.active_tab() == ResourceTab::CustomResources {
                refresh_tab(app, gateway, ResourceTab::CustomResources).await;
            }
        }
        AppCommand::LoadPodLogs {
            namespace,
            pod_name,
            container,
            previous,
        } => match gateway
            .fetch_pod_logs(&namespace, &pod_name, container.as_deref(), previous)
            .await
        {
            Ok(logs) => {
                let title = match (container.as_deref(), previous) {
                    (Some(container), true) => {
                        format!("Pod Logs (previous) {namespace}/{pod_name}:{container}")
                    }
                    (Some(container), false) => {
                        format!("Pod Logs {namespace}/{pod_name}:{container}")
                    }
                    (None, true) => format!("Pod Logs (previous) {namespace}/{pod_name}"),
                    (None, false) => format!("Pod Logs {namespace}/{pod_name}"),
                };
                app.set_pod_logs_overlay(title, logs);
                app.set_status(format!("Loaded logs for {namespace}/{pod_name}"));
            }
            Err(error) => {
                app.set_status(format!(
                    "Failed loading logs for {namespace}/{pod_name}: {error:#}"
                ));
            }
        },
        AppCommand::LoadResourceLogs {
            tab,
            namespace,
            name,
            previous,
        } => match gateway
            .resolve_log_target(tab, namespace.as_deref(), &name)
            .await
        {
            Ok(target) => match gateway
                .fetch_pod_logs(
                    &target.namespace,
                    &target.pod_name,
                    target.container.as_deref(),
                    previous,
                )
                .await
            {
                Ok(logs) => {
                    let title = match (target.container.as_deref(), previous) {
                        (Some(container), true) => format!(
                            "Logs (previous) {}/{}:{}",
                            target.namespace, target.pod_name, container
                        ),
                        (Some(container), false) => {
                            format!(
                                "Logs {}/{}:{}",
                                target.namespace, target.pod_name, container
                            )
                        }
                        (None, true) => {
                            format!("Logs (previous) {}/{}", target.namespace, target.pod_name)
                        }
                        (None, false) => format!("Logs {}/{}", target.namespace, target.pod_name),
                    };
                    app.set_related_logs_overlay(title, logs);
                    app.set_status(format!(
                        "Loaded related logs via {} for {}/{}",
                        target.source, target.namespace, target.pod_name
                    ));
                }
                Err(error) => app.set_status(format!(
                    "Failed loading related logs for {}/{}: {error:#}",
                    target.namespace, target.pod_name
                )),
            },
            Err(error) => {
                app.set_status(format!(
                    "Failed resolving logs for {} {}: {error:#}",
                    tab.title(),
                    name
                ));
            }
        },
        AppCommand::LoadPodContainers {
            namespace,
            pod_name,
        } => match gateway.pod_containers(&namespace, &pod_name).await {
            Ok(containers) => {
                app.set_container_picker(namespace.clone(), pod_name.clone(), containers);
                app.set_status(format!("Loaded containers for {namespace}/{pod_name}"));
            }
            Err(error) => {
                app.set_status(format!(
                    "Failed loading containers for {namespace}/{pod_name}: {error:#}"
                ));
            }
        },
        AppCommand::DeleteSelected {
            tab,
            namespace,
            name,
        } => match gateway
            .delete_resource(tab, namespace.as_deref(), &name)
            .await
        {
            Ok(()) => {
                match namespace {
                    Some(namespace) => {
                        app.set_status(format!("Deleted {} {}/{}", tab.title(), namespace, name))
                    }
                    None => app.set_status(format!("Deleted {} {}", tab.title(), name)),
                }
                refresh_tab(app, gateway, tab).await;
            }
            Err(error) => app.set_status(format!(
                "Delete failed for {} {}: {error:#}",
                tab.title(),
                name
            )),
        },
        AppCommand::RestartWorkload {
            tab,
            namespace,
            name,
        } => match gateway.restart_workload(tab, &namespace, &name).await {
            Ok(()) => {
                app.set_status(format!(
                    "Restart triggered for {} {}/{}",
                    tab.title(),
                    namespace,
                    name
                ));
                refresh_tab(app, gateway, tab).await;
            }
            Err(error) => app.set_status(format!(
                "Restart failed for {} {}/{}: {error:#}",
                tab.title(),
                namespace,
                name
            )),
        },
        AppCommand::ScaleWorkload {
            tab,
            namespace,
            name,
            replicas,
        } => match gateway
            .scale_workload(tab, &namespace, &name, replicas)
            .await
        {
            Ok(()) => {
                app.set_status(format!(
                    "Scaled {} {}/{} to {} replicas",
                    tab.title(),
                    namespace,
                    name,
                    replicas
                ));
                refresh_tab(app, gateway, tab).await;
            }
            Err(error) => app.set_status(format!(
                "Scale failed for {} {}/{}: {error:#}",
                tab.title(),
                namespace,
                name
            )),
        },
        AppCommand::ExecInPod {
            namespace,
            pod_name,
            command,
        } => match run_kubectl_exec(&namespace, &pod_name, &command).await {
            Ok(output) => {
                app.set_detail_overlay("Exec Output", output);
                app.set_status(format!("Exec completed for {namespace}/{pod_name}"));
            }
            Err(error) => {
                app.set_status(format!("Exec failed for {namespace}/{pod_name}: {error:#}"))
            }
        },
        AppCommand::OpenPodShell {
            namespace,
            pod_name,
            container,
            shell,
        } => match run_kubectl_shell(
            terminal,
            &namespace,
            &pod_name,
            container.as_deref(),
            &shell,
        )
        .await
        {
            Ok(()) => {
                app.set_status(format!("Shell session closed for {namespace}/{pod_name}"));
                if app.active_tab() == ResourceTab::Pods {
                    refresh_tab(app, gateway, ResourceTab::Pods).await;
                }
            }
            Err(error) => app.set_status(format!(
                "Shell failed for {namespace}/{pod_name}: {error:#}"
            )),
        },
        AppCommand::EditSelected {
            resource,
            namespace,
            name,
        } => match run_kubectl_edit(terminal, &resource, namespace.as_deref(), &name).await {
            Ok(()) => {
                app.set_status(match namespace {
                    Some(namespace) => format!("Edited {resource} {namespace}/{name}"),
                    None => format!("Edited {resource} {name}"),
                });
                refresh_tab(app, gateway, app.active_tab()).await;
            }
            Err(error) => app.set_status(format!("Edit failed for {resource} {name}: {error:#}")),
        },
        AppCommand::StartPortForward {
            tab,
            namespace,
            name,
            local_port,
            remote_port,
        } => {
            match run_kubectl_port_forward(tab, &namespace, &name, local_port, remote_port).await {
                Ok((pid, mut child)) => {
                    app.register_port_forward(
                        tab,
                        namespace.clone(),
                        name.clone(),
                        local_port,
                        remote_port,
                        pid,
                    );
                    let target = match tab {
                        ResourceTab::Pods => format!("pod/{name}"),
                        ResourceTab::Services => format!("service/{name}"),
                        _ => name.clone(),
                    };
                    app.set_status(format!(
                        "Port-forward started ({target}) {local_port}:{remote_port} pid={pid}"
                    ));

                    let tx = pf_tx.clone();
                    tokio::spawn(async move {
                        let result = child
                            .wait()
                            .await
                            .map_err(|error| format!("wait failed: {error}"));
                        let _ = tx.send(PortForwardExitEvent {
                            pid,
                            tab,
                            namespace,
                            name,
                            local_port,
                            remote_port,
                            result,
                        });
                    });
                }
                Err(error) => app.set_status(format!(
                    "Port-forward failed for {} {}/{}: {error:#}",
                    tab.title(),
                    namespace,
                    name
                )),
            }
        }
        AppCommand::SwitchContext { context } => match gateway.switch_context(&context).await {
            Ok(()) => {
                app.set_kube_target(
                    gateway.cluster().to_string(),
                    gateway.context().to_string(),
                    gateway.user().to_string(),
                    gateway.default_namespace().to_string(),
                    true,
                );
                app.set_kube_catalog(
                    gateway.available_contexts(),
                    gateway.available_clusters(),
                    gateway.available_users(),
                );
                refresh_custom_resource_catalog(app, gateway).await;
                let tabs = app.tabs().to_vec();
                for tab in tabs {
                    refresh_tab(app, gateway, tab).await;
                }
                app.set_status(format!(
                    "Switched context to '{}' ({})",
                    gateway.context(),
                    gateway.cluster()
                ));
                return LoopEffect::RestartWatchers;
            }
            Err(error) => {
                app.set_status(format!("Context switch failed for '{context}': {error:#}"))
            }
        },
        AppCommand::SwitchCluster { cluster } => match gateway.switch_cluster(&cluster).await {
            Ok(context) => {
                app.set_kube_target(
                    gateway.cluster().to_string(),
                    gateway.context().to_string(),
                    gateway.user().to_string(),
                    gateway.default_namespace().to_string(),
                    true,
                );
                app.set_kube_catalog(
                    gateway.available_contexts(),
                    gateway.available_clusters(),
                    gateway.available_users(),
                );
                refresh_custom_resource_catalog(app, gateway).await;
                let tabs = app.tabs().to_vec();
                for tab in tabs {
                    refresh_tab(app, gateway, tab).await;
                }
                app.set_status(format!(
                    "Switched cluster '{}' via context '{}' ({})",
                    cluster,
                    context,
                    gateway.cluster()
                ));
                return LoopEffect::RestartWatchers;
            }
            Err(error) => {
                app.set_status(format!("Cluster switch failed for '{cluster}': {error:#}"))
            }
        },
        AppCommand::SwitchUser { user } => match gateway.switch_user(&user).await {
            Ok(context) => {
                app.set_kube_target(
                    gateway.cluster().to_string(),
                    gateway.context().to_string(),
                    gateway.user().to_string(),
                    gateway.default_namespace().to_string(),
                    true,
                );
                app.set_kube_catalog(
                    gateway.available_contexts(),
                    gateway.available_clusters(),
                    gateway.available_users(),
                );
                refresh_custom_resource_catalog(app, gateway).await;
                let tabs = app.tabs().to_vec();
                for tab in tabs {
                    refresh_tab(app, gateway, tab).await;
                }
                app.set_status(format!(
                    "Switched user '{}' via context '{}' ({})",
                    user,
                    context,
                    gateway.cluster()
                ));
                return LoopEffect::RestartWatchers;
            }
            Err(error) => app.set_status(format!("User switch failed for '{user}': {error:#}")),
        },
    }

    LoopEffect::None
}

async fn refresh_tab(app: &mut App, gateway: &KubeGateway, tab: ResourceTab) {
    let scope = app.namespace_scope().clone();
    let selected_custom = app.selected_custom_resource().cloned();
    match timeout(
        TABLE_REFRESH_TIMEOUT,
        gateway.fetch_table(tab, &scope, selected_custom.as_ref()),
    )
    .await
    {
        Ok(Ok(table)) => {
            app.set_active_table_data(tab, table);
            if tab == app.active_tab() {
                match timeout(
                    METRICS_REFRESH_TIMEOUT,
                    gateway.fetch_overview_metrics(&scope),
                )
                .await
                {
                    Ok(Ok(metrics)) => app.set_overview_metrics(metrics),
                    Ok(Err(error)) => {
                        app.set_status(format!(
                            "Metrics refresh failed for {}: {}",
                            tab.title(),
                            compact_error(&error)
                        ));
                    }
                    Err(_) => {
                        app.set_status(format!(
                            "Metrics refresh timed out for {} (using cached)",
                            tab.title()
                        ));
                    }
                }
            }
        }
        Ok(Err(error)) => app.set_active_tab_error(tab, compact_error(&error)),
        Err(_) => {
            app.set_status(format!(
                "Refresh timed out for {} (showing cached data)",
                tab.title()
            ));
        }
    }
}

async fn refresh_custom_resource_catalog(app: &mut App, gateway: &KubeGateway) {
    match timeout(CRD_DISCOVERY_TIMEOUT, gateway.discover_custom_resources()).await {
        Ok(Ok(crds)) => app.set_custom_resources(crds),
        Ok(Err(error)) => app.set_status(format!("CRD discovery failed: {error:#}")),
        Err(_) => app.set_status("CRD discovery timed out (using cached)"),
    }
}

async fn run_kubectl_exec(namespace: &str, pod_name: &str, command: &[String]) -> Result<String> {
    let mut cmd = TokioCommand::new("kubectl");
    cmd.arg("exec")
        .arg("-n")
        .arg(namespace)
        .arg(pod_name)
        .arg("--")
        .args(command)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = cmd
        .output()
        .await
        .with_context(|| format!("failed to execute kubectl for {namespace}/{pod_name}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let rendered = if stderr.trim().is_empty() {
        stdout.to_string()
    } else if stdout.trim().is_empty() {
        format!("stderr:\n{stderr}")
    } else {
        format!("stdout:\n{stdout}\n\nstderr:\n{stderr}")
    };

    if output.status.success() {
        Ok(rendered)
    } else {
        Err(anyhow::anyhow!(
            "kubectl exec exited with {}",
            output.status
        ))
    }
}

async fn run_kubectl_shell(
    terminal: &mut TuiTerminal,
    namespace: &str,
    pod_name: &str,
    container: Option<&str>,
    shell: &str,
) -> Result<()> {
    suspend_terminal_for_subprocess(terminal)?;

    let mut cmd = TokioCommand::new("kubectl");
    cmd.arg("exec")
        .arg("-it")
        .arg("-n")
        .arg(namespace)
        .arg(pod_name);
    if let Some(container) = container {
        cmd.arg("-c").arg(container);
    }
    cmd.arg("--")
        .arg(shell)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let run_result = cmd
        .status()
        .await
        .with_context(|| format!("failed to run kubectl shell for {namespace}/{pod_name}"));
    let restore_result = resume_terminal_after_subprocess(terminal);

    let status = match (run_result, restore_result) {
        (Err(run_error), Err(restore_error)) => {
            return Err(anyhow::anyhow!(
                "{run_error:#}\nterminal resume error: {restore_error:#}"
            ));
        }
        (Err(error), _) => return Err(error),
        (_, Err(error)) => return Err(error),
        (Ok(status), Ok(())) => status,
    };

    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("kubectl shell exited with {status}"))
    }
}

async fn run_kubectl_edit(
    terminal: &mut TuiTerminal,
    resource: &str,
    namespace: Option<&str>,
    name: &str,
) -> Result<()> {
    suspend_terminal_for_subprocess(terminal)?;

    let mut cmd = TokioCommand::new("kubectl");
    cmd.arg("edit").arg(resource).arg(name);
    if let Some(namespace) = namespace {
        cmd.arg("-n").arg(namespace);
    }
    if std::env::var_os("KUBE_EDITOR").is_none()
        && let Some(editor) = std::env::var_os("EDITOR")
    {
        cmd.env("KUBE_EDITOR", editor);
    }
    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let run_result = cmd
        .status()
        .await
        .with_context(|| format!("failed to run kubectl edit for {resource} {name}"));
    let restore_result = resume_terminal_after_subprocess(terminal);

    let status = match (run_result, restore_result) {
        (Err(run_error), Err(restore_error)) => {
            return Err(anyhow::anyhow!(
                "{run_error:#}\nterminal resume error: {restore_error:#}"
            ));
        }
        (Err(error), _) => return Err(error),
        (_, Err(error)) => return Err(error),
        (Ok(status), Ok(())) => status,
    };

    if status.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("kubectl edit exited with {status}"))
    }
}

fn suspend_terminal_for_subprocess(terminal: &mut TuiTerminal) -> Result<()> {
    disable_raw_mode().context("failed to disable raw mode for subprocess")?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)
        .context("failed to leave alternate screen for subprocess")?;
    terminal
        .show_cursor()
        .context("failed to show cursor for subprocess")?;
    Ok(())
}

fn resume_terminal_after_subprocess(terminal: &mut TuiTerminal) -> Result<()> {
    enable_raw_mode().context("failed to re-enable raw mode after subprocess")?;
    execute!(terminal.backend_mut(), EnterAlternateScreen)
        .context("failed to re-enter alternate screen after subprocess")?;
    terminal
        .clear()
        .context("failed to clear terminal after subprocess")?;
    Ok(())
}

async fn run_kubectl_port_forward(
    tab: ResourceTab,
    namespace: &str,
    name: &str,
    local_port: u16,
    remote_port: u16,
) -> Result<(u32, tokio::process::Child)> {
    let target = match tab {
        ResourceTab::Pods => format!("pod/{name}"),
        ResourceTab::Services => format!("service/{name}"),
        _ => anyhow::bail!("port-forward only supports pods and services"),
    };

    let child = TokioCommand::new("kubectl")
        .arg("port-forward")
        .arg("-n")
        .arg(namespace)
        .arg(&target)
        .arg(format!("{local_port}:{remote_port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to spawn port-forward for {namespace}/{target}"))?;

    let pid = child
        .id()
        .context("failed to determine process id for kubectl port-forward")?;

    Ok((pid, child))
}

fn should_process_watch_event(
    tab: ResourceTab,
    throttle: &mut HashMap<ResourceTab, Instant>,
) -> bool {
    let now = Instant::now();
    let min_interval = Duration::from_millis(350);
    let Some(last) = throttle.get(&tab) else {
        throttle.insert(tab, now);
        return true;
    };

    if now.duration_since(*last) >= min_interval {
        throttle.insert(tab, now);
        true
    } else {
        false
    }
}

fn restart_watchers(
    watch_tasks: &mut Vec<JoinHandle<()>>,
    client: Client,
    tx: mpsc::UnboundedSender<ResourceTab>,
) {
    for task in watch_tasks.drain(..) {
        task.abort();
    }
    *watch_tasks = start_resource_watchers(client, tx);
}

fn start_resource_watchers(
    client: Client,
    tx: mpsc::UnboundedSender<ResourceTab>,
) -> Vec<JoinHandle<()>> {
    vec![
        spawn_watch_task::<Pod>(client.clone(), ResourceTab::Pods, tx.clone()),
        spawn_watch_task::<CronJob>(client.clone(), ResourceTab::CronJobs, tx.clone()),
        spawn_watch_task::<DaemonSet>(client.clone(), ResourceTab::DaemonSets, tx.clone()),
        spawn_watch_task::<Deployment>(client.clone(), ResourceTab::Deployments, tx.clone()),
        spawn_watch_task::<ReplicaSet>(client.clone(), ResourceTab::ReplicaSets, tx.clone()),
        spawn_watch_task::<ReplicationController>(
            client.clone(),
            ResourceTab::ReplicationControllers,
            tx.clone(),
        ),
        spawn_watch_task::<StatefulSet>(client.clone(), ResourceTab::StatefulSets, tx.clone()),
        spawn_watch_task::<Job>(client.clone(), ResourceTab::Jobs, tx.clone()),
        spawn_watch_task::<Service>(client.clone(), ResourceTab::Services, tx.clone()),
        spawn_watch_task::<Ingress>(client.clone(), ResourceTab::Ingresses, tx.clone()),
        spawn_watch_task::<IngressClass>(client.clone(), ResourceTab::IngressClasses, tx.clone()),
        spawn_watch_task::<ConfigMap>(client.clone(), ResourceTab::ConfigMaps, tx.clone()),
        spawn_watch_task::<PersistentVolumeClaim>(
            client.clone(),
            ResourceTab::PersistentVolumeClaims,
            tx.clone(),
        ),
        spawn_watch_task::<Secret>(client.clone(), ResourceTab::Secrets, tx.clone()),
        spawn_watch_task::<StorageClass>(client.clone(), ResourceTab::StorageClasses, tx.clone()),
        spawn_watch_task::<PersistentVolume>(
            client.clone(),
            ResourceTab::PersistentVolumes,
            tx.clone(),
        ),
        spawn_watch_task::<ServiceAccount>(
            client.clone(),
            ResourceTab::ServiceAccounts,
            tx.clone(),
        ),
        spawn_watch_task::<Role>(client.clone(), ResourceTab::Roles, tx.clone()),
        spawn_watch_task::<RoleBinding>(client.clone(), ResourceTab::RoleBindings, tx.clone()),
        spawn_watch_task::<ClusterRole>(client.clone(), ResourceTab::ClusterRoles, tx.clone()),
        spawn_watch_task::<ClusterRoleBinding>(
            client.clone(),
            ResourceTab::ClusterRoleBindings,
            tx.clone(),
        ),
        spawn_watch_task::<NetworkPolicy>(client.clone(), ResourceTab::NetworkPolicies, tx.clone()),
        spawn_watch_task::<Node>(client.clone(), ResourceTab::Nodes, tx.clone()),
        spawn_watch_task::<KubeEvent>(client.clone(), ResourceTab::Events, tx.clone()),
        spawn_watch_task::<Namespace>(client, ResourceTab::Namespaces, tx),
    ]
}

fn spawn_watch_task<K>(
    client: Client,
    tab: ResourceTab,
    tx: mpsc::UnboundedSender<ResourceTab>,
) -> JoinHandle<()>
where
    K: Clone + std::fmt::Debug + serde::de::DeserializeOwned + kube::Resource + Send + 'static,
    <K as kube::Resource>::DynamicType: Default + Eq + std::hash::Hash + Clone + Send,
{
    tokio::spawn(async move {
        loop {
            let api: Api<K> = Api::all(client.clone());
            let mut events = watcher(api, WatchConfig::default()).boxed();
            loop {
                match events.try_next().await {
                    Ok(Some(_)) => {
                        let _ = tx.send(tab);
                    }
                    Ok(None) => break,
                    Err(error) => {
                        warn!("watch stream error for {}: {error}", tab.title());
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(900)).await;
        }
    })
}

fn compact_error(error: &anyhow::Error) -> String {
    let mut out = Vec::new();
    for (index, cause) in error.chain().enumerate() {
        if index == 0 {
            out.push(cause.to_string());
        } else if index <= 2 {
            out.push(format!("caused by: {cause}"));
        } else {
            break;
        }
    }

    out.join("\n")
}
