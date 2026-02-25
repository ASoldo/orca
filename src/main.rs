mod app;
mod cli;
mod config;
mod input;
mod k8s;
mod model;
mod ui;

use anyhow::{Context, Result};
use app::{App, AppCommand, ArgoResourcePanelSection, OpsInspectTarget, PluginRun};
use chrono::Local;
use clap::Parser;
use cli::CliArgs;
use crossterm::event::{
    Event, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, KeyboardEnhancementFlags,
    PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
    supports_keyboard_enhancement,
};
use futures::{StreamExt, TryStreamExt};
use input::key_event_signature;
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
use model::{RowData, TableData};
use portable_pty::{CommandBuilder as PtyCommandBuilder, PtySize, native_pty_system};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read, Stdout, Write};
use std::path::{Path, PathBuf};
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

#[derive(Debug, Clone)]
struct ShellOutputEvent {
    snapshot: String,
    application_cursor: bool,
}

#[derive(Default)]
struct EmbeddedShellState {
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
    writer: Option<Box<dyn Write + Send>>,
    application_cursor: bool,
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
    if std::env::var("ORCA_READONLY")
        .map(|value| parse_truthy_env(&value))
        .unwrap_or(false)
    {
        app.set_read_only(true);
    }
    app.set_user(gateway.user().to_string());
    app.set_kube_catalog(
        gateway.available_contexts(),
        gateway.available_clusters(),
        gateway.available_users(),
        gateway.context_catalog(),
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

fn parse_truthy_env(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | "enabled" | "enable"
    )
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
    let mut config_watcher = config::RuntimeConfigWatcher::discover();
    match config_watcher.load_current() {
        Ok(snapshot) => {
            app.set_runtime_config(
                snapshot.aliases,
                snapshot.plugins,
                snapshot.hotkeys,
                snapshot.source.clone(),
            );
        }
        Err(error) => {
            app.set_runtime_config(HashMap::new(), Vec::new(), Vec::new(), None);
            app.set_status(format!(
                "Runtime config load failed: {}",
                compact_error(&error)
            ));
        }
    }

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
    let (shell_output_tx, mut shell_output_rx) = mpsc::unbounded_channel::<ShellOutputEvent>();
    let mut embedded_shell = EmbeddedShellState::default();

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
                        if app.shell_overlay_active()
                            && app.mode() == app::InputMode::Normal
                            && key.code != KeyCode::Esc
                        {
                            let _ = forward_key_to_embedded_shell(
                                key,
                                &mut embedded_shell.writer,
                                embedded_shell.application_cursor,
                            );
                            continue;
                        }

                        if app.mode() == app::InputMode::Normal
                            && let Some(signature) = key_event_signature(key.clone())
                            && let Some(command) = app.execute_hotkey_signature(&signature)
                        {
                            let was_shell_open = app.shell_overlay_active();
                            terminal
                                .draw(|frame| ui::render(frame, app))
                                .context("failed to render terminal frame")?;
                            let effect =
                                execute_app_command(
                                    terminal,
                                    app,
                                    gateway,
                                    command,
                                    &pf_tx,
                                    &shell_output_tx,
                                    &mut embedded_shell,
                                )
                                .await;
                            if was_shell_open && !app.shell_overlay_active() {
                                stop_embedded_shell(&mut embedded_shell).await;
                            }
                            if matches!(effect, LoopEffect::RestartWatchers) {
                                restart_watchers(
                                    &mut watch_tasks,
                                    gateway.client(),
                                    watch_tx.clone(),
                                );
                                watch_throttle.clear();
                            }
                            continue;
                        }

                        if let Some(action) = input::map_key(app.mode(), key) {
                            debug!("action={action:?}");
                            let was_shell_open = app.shell_overlay_active();
                            let command = app.apply_action(action);
                            terminal
                                .draw(|frame| ui::render(frame, app))
                                .context("failed to render terminal frame")?;
                            let effect =
                                execute_app_command(
                                    terminal,
                                    app,
                                    gateway,
                                    command,
                                    &pf_tx,
                                    &shell_output_tx,
                                    &mut embedded_shell,
                                ).await;
                            if was_shell_open && !app.shell_overlay_active() {
                                stop_embedded_shell(&mut embedded_shell).await;
                            }
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
                match config_watcher.reload_if_changed() {
                    Ok(Some(snapshot)) => {
                        app.set_runtime_config(
                            snapshot.aliases,
                            snapshot.plugins,
                            snapshot.hotkeys,
                            snapshot.source.clone(),
                        );
                        let source = snapshot.source.unwrap_or_else(|| "(none)".to_string());
                        app.set_status(format!(
                            "Runtime config reloaded from {} (aliases:{} plugins:{} hotkeys:{})",
                            source,
                            app.runtime_alias_count(),
                            app.runtime_plugin_count(),
                            app.runtime_hotkey_count(),
                        ));
                    }
                    Ok(None) => {}
                    Err(error) => {
                        app.set_status(format!(
                            "Runtime config reload failed: {}",
                            compact_error(&error)
                        ));
                    }
                }

                let active = app.active_tab();
                refresh_tab(app, gateway, active).await;

                let mut should_reset_shell = false;
                if let Some(child) = embedded_shell.child.as_mut() {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            if app.shell_overlay_active() {
                                let _ = app.apply_action(input::Action::ClearDetailOverlay);
                            }
                            app.set_status(format!("Embedded shell exited: {status}"));
                            should_reset_shell = true;
                        }
                        Ok(None) => {}
                        Err(error) => {
                            app.set_status(format!("Embedded shell wait failed: {error}"));
                            should_reset_shell = true;
                        }
                    }
                }
                if should_reset_shell {
                    embedded_shell.child = None;
                    embedded_shell.writer = None;
                    embedded_shell.application_cursor = false;
                }
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
            maybe_shell_output = shell_output_rx.recv() => {
                if let Some(event) = maybe_shell_output
                    && app.shell_overlay_active() {
                    embedded_shell.application_cursor = event.application_cursor;
                    app.replace_shell_output(event.snapshot);
                }
            }
        }
    }

    stop_embedded_shell(&mut embedded_shell).await;
    Ok(())
}

async fn execute_app_command(
    terminal: &mut TuiTerminal,
    app: &mut App,
    gateway: &mut KubeGateway,
    command: AppCommand,
    pf_tx: &mpsc::UnboundedSender<PortForwardExitEvent>,
    shell_output_tx: &mpsc::UnboundedSender<ShellOutputEvent>,
    embedded_shell: &mut EmbeddedShellState,
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
        } => {
            let mut resolved_container = container.clone();
            if resolved_container.is_none()
                && let Ok(containers) = gateway.pod_containers(&namespace, &pod_name).await
            {
                resolved_container = containers.first().map(|entry| entry.name.clone());
            }

            match gateway
                .fetch_pod_logs(
                    &namespace,
                    &pod_name,
                    resolved_container.as_deref(),
                    previous,
                )
                .await
            {
                Ok(logs) => {
                    let title = match (resolved_container.as_deref(), previous) {
                        (Some(container), true) => {
                            format!("Container Logs (previous) {namespace}/{pod_name}:{container}")
                        }
                        (Some(container), false) => {
                            format!("Container Logs {namespace}/{pod_name}:{container}")
                        }
                        (None, true) => format!("Pod Logs (previous) {namespace}/{pod_name}"),
                        (None, false) => format!("Pod Logs {namespace}/{pod_name}"),
                    };
                    app.set_pod_logs_overlay(title, logs);
                    app.set_status(match resolved_container.as_deref() {
                        Some(container) => {
                            format!("Loaded container logs for {namespace}/{pod_name}:{container}")
                        }
                        None => format!("Loaded pod logs for {namespace}/{pod_name}"),
                    });
                }
                Err(error) => {
                    app.set_status(format!(
                        "Failed loading logs for {namespace}/{pod_name}: {error:#}"
                    ));
                }
            }
        }
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
        AppCommand::LoadArgoResourcePanel {
            kind,
            namespace,
            name,
        } => match fetch_argocd_resource_panel(&kind, namespace.as_deref(), &name).await {
            Ok((title, panel)) => {
                app.set_detail_overlay(title, panel);
                app.set_status(match namespace.as_deref() {
                    Some(namespace) => format!("Argo panel loaded for {kind} {namespace}/{name}"),
                    None => format!("Argo panel loaded for {kind} {name}"),
                });
            }
            Err(error) => {
                let title = match namespace.as_deref() {
                    Some(namespace) => format!("Argo {kind} {namespace}/{name}"),
                    None => format!("Argo {kind} {name}"),
                };
                app.set_detail_overlay(title, error.clone());
                app.set_status(format!("Argo panel load failed: {error}"));
            }
        },
        AppCommand::LoadArgoResourcePanelSection {
            kind,
            namespace,
            name,
            section,
        } => match fetch_argocd_resource_panel_sections(&kind, namespace.as_deref(), &name).await {
            Ok((title, sections)) => {
                let (panel_title, panel_text) = match section {
                    ArgoResourcePanelSection::Events => (
                        format!("{title} Events"),
                        format!("EVENTS\n{}", sections.events),
                    ),
                    ArgoResourcePanelSection::Manifest => (
                        format!("{title} Manifest"),
                        format!("LIVE MANIFEST\n{}", sections.manifest),
                    ),
                };
                app.set_output_overlay(panel_title, panel_text);
                app.set_status(match section {
                    ArgoResourcePanelSection::Events => format!(
                        "Loaded Argo events for {} {}/{}",
                        kind,
                        namespace.as_deref().unwrap_or("-"),
                        name
                    ),
                    ArgoResourcePanelSection::Manifest => format!(
                        "Loaded Argo manifest for {} {}/{}",
                        kind,
                        namespace.as_deref().unwrap_or("-"),
                        name
                    ),
                });
            }
            Err(error) => {
                let title = match namespace.as_deref() {
                    Some(namespace) => format!("Argo {kind} {namespace}/{name}"),
                    None => format!("Argo {kind} {name}"),
                };
                app.set_output_overlay(
                    match section {
                        ArgoResourcePanelSection::Events => format!("{title} Events"),
                        ArgoResourcePanelSection::Manifest => format!("{title} Manifest"),
                    },
                    error.clone(),
                );
                app.set_status(format!("Argo section load failed: {error}"));
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
        } => {
            stop_embedded_shell(embedded_shell).await;
            match start_embedded_kubectl_shell(&namespace, &pod_name, container.as_deref(), &shell)
            {
                Ok(started) => {
                    let title = match container.as_deref() {
                        Some(container) => {
                            format!("Shell {namespace}/{pod_name}:{container} ({shell})")
                        }
                        None => format!("Shell {namespace}/{pod_name} ({shell})"),
                    };
                    app.set_shell_overlay(
                        title,
                        "[orca] embedded shell started (Esc to close)\n".to_string(),
                    );

                    spawn_shell_reader(started.reader, shell_output_tx.clone());
                    embedded_shell.child = Some(started.child);
                    embedded_shell.writer = Some(started.writer);
                    embedded_shell.application_cursor = false;
                    app.set_status(format!(
                        "Embedded shell opened for {namespace}/{pod_name} (Esc to close)"
                    ));
                }
                Err(error) => app.set_status(format!(
                    "Shell failed for {namespace}/{pod_name}: {error:#}"
                )),
            }
        }
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
        AppCommand::InspectTooling => {
            let report = inspect_toolchain().await;
            app.set_output_overlay("Toolchain Inventory", report);
            app.set_status("Toolchain inventory refreshed");
        }
        AppCommand::InspectPulses => match gateway.fetch_pulses_report(app.namespace_scope()).await
        {
            Ok(report) => {
                app.set_output_overlay("Pulses", report);
                app.set_status("Pulses snapshot refreshed");
            }
            Err(error) => {
                app.set_status(format!("Pulses refresh failed: {error:#}"));
            }
        },
        AppCommand::InspectAlerts => match gateway.fetch_alerts_report(app.namespace_scope()).await
        {
            Ok(report) => {
                app.set_output_overlay("Alerts", report);
                app.set_status("Alerts snapshot refreshed");
            }
            Err(error) => {
                app.set_status(format!("Alerts refresh failed: {error:#}"));
            }
        },
        AppCommand::InspectOps { target } => {
            let refresh_target = target.clone();
            let (title, report, status) = inspect_ops_target(target, app.namespace_scope()).await;
            app.set_output_overlay(title, report);
            app.set_status(status);
            if matches!(
                refresh_target,
                OpsInspectTarget::ArgoCdSync { .. }
                    | OpsInspectTarget::ArgoCdRefresh { .. }
                    | OpsInspectTarget::ArgoCdRollback { .. }
                    | OpsInspectTarget::ArgoCdDelete { .. }
            ) {
                refresh_tab(app, gateway, ResourceTab::ArgoCdApps).await;
                if matches!(
                    app.active_tab(),
                    ResourceTab::ArgoCdResources
                        | ResourceTab::ArgoCdApps
                        | ResourceTab::ArgoCdProjects
                        | ResourceTab::ArgoCdRepos
                        | ResourceTab::ArgoCdClusters
                        | ResourceTab::ArgoCdAccounts
                        | ResourceTab::ArgoCdCerts
                        | ResourceTab::ArgoCdGpgKeys
                ) {
                    let active = app.active_tab();
                    refresh_tab(app, gateway, active).await;
                }
            }
        }
        AppCommand::InspectXray {
            tab,
            namespace,
            name,
        } => match gateway
            .fetch_xray_report(tab, namespace.as_deref(), &name)
            .await
        {
            Ok(report) => {
                let title = match namespace {
                    Some(namespace) => format!("Xray {} {namespace}/{name}", tab.title()),
                    None => format!("Xray {} {name}", tab.title()),
                };
                app.set_output_overlay(title, report);
                app.set_status(format!("Xray refreshed for {} {}", tab.title(), name));
            }
            Err(error) => {
                app.set_status(format!(
                    "Xray failed for {} {}: {error:#}",
                    tab.title(),
                    name
                ));
            }
        },
        AppCommand::RunPlugin { run } => match run_plugin_command(&run).await {
            Ok(output) => {
                app.set_output_overlay(format!("Plugin {}", run.name), output);
                app.set_status(format!("Plugin '{}' finished", run.name));
            }
            Err(error) => {
                app.set_output_overlay(format!("Plugin {}", run.name), format!("{error:#}"));
                app.set_status(format!("Plugin '{}' failed", run.name));
            }
        },
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
                    gateway.context_catalog(),
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
                    gateway.context_catalog(),
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
                    gateway.context_catalog(),
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

struct ToolProbe {
    name: &'static str,
    program: &'static str,
    args: &'static [&'static str],
}

async fn inspect_toolchain() -> String {
    let probes = [
        ToolProbe {
            name: "kubectl",
            program: "kubectl",
            args: &["version", "--client=true"],
        },
        ToolProbe {
            name: "oc",
            program: "oc",
            args: &["version", "--client=true"],
        },
        ToolProbe {
            name: "helm",
            program: "helm",
            args: &["version", "--short"],
        },
        ToolProbe {
            name: "argocd",
            program: "argocd",
            args: &["version", "--client", "--short"],
        },
        ToolProbe {
            name: "terraform",
            program: "terraform",
            args: &["version"],
        },
        ToolProbe {
            name: "ansible-playbook",
            program: "ansible-playbook",
            args: &["--version"],
        },
        ToolProbe {
            name: "docker",
            program: "docker",
            args: &["--version"],
        },
        ToolProbe {
            name: "git",
            program: "git",
            args: &["--version"],
        },
        ToolProbe {
            name: "kustomize",
            program: "kustomize",
            args: &["version"],
        },
        ToolProbe {
            name: "kubectl-who-can",
            program: "kubectl-who-can",
            args: &["--help"],
        },
    ];

    let mut lines = vec![format!("{:<18} {:<10} {}", "TOOL", "STATUS", "DETAIL")];
    for probe in probes {
        match probe_tool_version(&probe).await {
            Ok(detail) => lines.push(format!(
                "{:<18} {:<10} {}",
                probe.name,
                "ok",
                fit_text(&detail, 120)
            )),
            Err(error) => lines.push(format!(
                "{:<18} {:<10} {}",
                probe.name,
                "missing",
                fit_text(&error, 120)
            )),
        }
    }

    lines.push(String::new());
    lines.push("Use :ctx / :cluster / :usr for kube target catalogs.".to_string());
    lines.push(
        "Use :git/:repo to fetch repos, inspect files, export overrides, and kubectl apply."
            .to_string(),
    );
    lines.push(
        "Use :shell (pods), :logs, :scale, :restart, :port-forward from resource tables."
            .to_string(),
    );
    lines.join("\n")
}

async fn probe_tool_version(probe: &ToolProbe) -> std::result::Result<String, String> {
    let mut cmd = TokioCommand::new(probe.program);
    cmd.args(probe.args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = timeout(Duration::from_secs(3), cmd.output())
        .await
        .map_err(|_| "timeout".to_string())?
        .map_err(|error| error.to_string())?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let detail = first_non_empty_line(if stdout.is_empty() { &stderr } else { &stdout });

    if output.status.success() {
        Ok(detail.unwrap_or_else(|| "available".to_string()))
    } else if let Some(detail) = detail {
        Err(detail)
    } else {
        Err(format!("exit {}", output.status))
    }
}

fn first_non_empty_line(input: &str) -> Option<String> {
    input
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(|line| line.trim().to_string())
}

fn fit_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }
    if max_chars <= 1 {
        return "…".to_string();
    }

    let mut out = input
        .chars()
        .take(max_chars.saturating_sub(1))
        .collect::<String>();
    out.push('…');
    out
}

async fn inspect_ops_target(
    target: OpsInspectTarget,
    namespace_scope: &NamespaceScope,
) -> (String, String, String) {
    match target {
        OpsInspectTarget::ArgoCdSync { name } => {
            let args = vec!["app".to_string(), "sync".to_string(), name.clone()];
            match run_external_readonly("argocd", &args, 30).await {
                Ok(output) => (
                    format!("Argo CD Sync {name}"),
                    bounded_output(&output, 260, 220),
                    format!("Argo CD sync completed: {name}"),
                ),
                Err(error) => (
                    format!("Argo CD Sync {name}"),
                    error.clone(),
                    format!("Argo CD sync failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::ArgoCdRefresh { name } => {
            let args = vec![
                "app".to_string(),
                "get".to_string(),
                name.clone(),
                "--refresh".to_string(),
            ];
            match run_external_readonly("argocd", &args, 15).await {
                Ok(output) => (
                    format!("Argo CD Refresh {name}"),
                    bounded_output(&output, 220, 220),
                    format!("Argo CD refresh completed: {name}"),
                ),
                Err(error) => (
                    format!("Argo CD Refresh {name}"),
                    error.clone(),
                    format!("Argo CD refresh failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::ArgoCdDiff { name } => {
            let args = vec!["app".to_string(), "diff".to_string(), name.clone()];
            match run_external_readonly("argocd", &args, 20).await {
                Ok(output) => (
                    format!("Argo CD Diff {name}"),
                    bounded_output(&output, 320, 220),
                    format!("Argo CD diff loaded: {name}"),
                ),
                Err(error) => (
                    format!("Argo CD Diff {name}"),
                    error.clone(),
                    format!("Argo CD diff failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::ArgoCdHistory { name } => {
            let args = vec![
                "app".to_string(),
                "history".to_string(),
                name.clone(),
                "-o".to_string(),
                "json".to_string(),
            ];
            match run_external_readonly("argocd", &args, 12).await {
                Ok(output) => (
                    format!("Argo CD History {name}"),
                    bounded_output(&output, 220, 220),
                    format!("Argo CD history loaded: {name}"),
                ),
                Err(error) => (
                    format!("Argo CD History {name}"),
                    error.clone(),
                    format!("Argo CD history failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::ArgoCdRollback { name, id } => {
            let args = vec![
                "app".to_string(),
                "rollback".to_string(),
                name.clone(),
                id.clone(),
            ];
            match run_external_readonly("argocd", &args, 20).await {
                Ok(output) => (
                    format!("Argo CD Rollback {name}#{id}"),
                    bounded_output(&output, 220, 220),
                    format!("Argo CD rollback completed: {name}#{id}"),
                ),
                Err(error) => (
                    format!("Argo CD Rollback {name}#{id}"),
                    error.clone(),
                    format!("Argo CD rollback failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::ArgoCdDelete { name } => {
            let args = vec![
                "app".to_string(),
                "delete".to_string(),
                name.clone(),
                "--yes".to_string(),
            ];
            match run_external_readonly("argocd", &args, 20).await {
                Ok(output) => (
                    format!("Argo CD Delete {name}"),
                    bounded_output(&output, 220, 220),
                    format!("Argo CD app deleted: {name}"),
                ),
                Err(error) => (
                    format!("Argo CD Delete {name}"),
                    error.clone(),
                    format!("Argo CD delete failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::HelmReleases => {
            let args = vec!["list".to_string(), "-A".to_string()];
            match run_external_readonly("helm", &args, 6).await {
                Ok(output) => (
                    "Helm Releases".to_string(),
                    bounded_output(&output, 220, 220),
                    "Helm release list loaded".to_string(),
                ),
                Err(error) => (
                    "Helm Releases".to_string(),
                    error,
                    "Helm release list failed".to_string(),
                ),
            }
        }
        OpsInspectTarget::HelmRelease { name } => {
            let mut args = vec!["status".to_string(), name.clone()];
            if let NamespaceScope::Named(namespace) = namespace_scope {
                args.push("-n".to_string());
                args.push(namespace.clone());
            }
            match run_external_readonly("helm", &args, 6).await {
                Ok(output) => (
                    format!("Helm Release {}", name),
                    bounded_output(&output, 280, 220),
                    format!("Helm release loaded: {name}"),
                ),
                Err(error) => (
                    format!("Helm Release {}", name),
                    error,
                    format!("Helm release lookup failed: {name}"),
                ),
            }
        }
        OpsInspectTarget::TerraformOverview => {
            let mut sections = Vec::new();
            sections.push(
                match run_external_readonly(
                    "terraform",
                    &["workspace".to_string(), "show".to_string()],
                    5,
                )
                .await
                {
                    Ok(output) => format!("workspace(show)\n{}", bounded_output(&output, 12, 220)),
                    Err(error) => format!("workspace(show)\n{error}"),
                },
            );
            sections.push(
                match run_external_readonly(
                    "terraform",
                    &["workspace".to_string(), "list".to_string()],
                    5,
                )
                .await
                {
                    Ok(output) => format!("workspace(list)\n{}", bounded_output(&output, 80, 220)),
                    Err(error) => format!("workspace(list)\n{error}"),
                },
            );
            sections.push(
                match run_external_readonly(
                    "terraform",
                    &["state".to_string(), "list".to_string()],
                    6,
                )
                .await
                {
                    Ok(output) => format!("state(list)\n{}", bounded_output(&output, 140, 220)),
                    Err(error) => format!("state(list)\n{error}"),
                },
            );
            (
                "Terraform Overview".to_string(),
                sections.join("\n\n"),
                "Terraform overview loaded".to_string(),
            )
        }
        OpsInspectTarget::AnsibleOverview => {
            let version = match run_external_readonly(
                "ansible-playbook",
                &["--version".to_string()],
                5,
            )
            .await
            {
                Ok(output) => format!("ansible-playbook\n{}", bounded_output(&output, 14, 220)),
                Err(error) => format!("ansible-playbook\n{error}"),
            };

            let playbooks = discover_ansible_playbooks(".", 6, 220);
            let playbook_lines = if playbooks.is_empty() {
                "No playbook-like files found under current path".to_string()
            } else {
                playbooks
                    .into_iter()
                    .map(|entry| fit_text(&entry, 220))
                    .collect::<Vec<_>>()
                    .join("\n")
            };

            (
                "Ansible Overview".to_string(),
                format!("{version}\n\nplaybooks\n{playbook_lines}"),
                "Ansible overview loaded".to_string(),
            )
        }
        OpsInspectTarget::DockerOverview => {
            let ps = match run_external_readonly(
                "docker",
                &[
                    "ps".to_string(),
                    "--format".to_string(),
                    "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}".to_string(),
                ],
                6,
            )
            .await
            {
                Ok(output) => format!("containers\n{}", bounded_output(&output, 80, 220)),
                Err(error) => format!("containers\n{error}"),
            };

            let images = match run_external_readonly(
                "docker",
                &[
                    "images".to_string(),
                    "--format".to_string(),
                    "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}".to_string(),
                ],
                6,
            )
            .await
            {
                Ok(output) => format!("images\n{}", bounded_output(&output, 80, 220)),
                Err(error) => format!("images\n{error}"),
            };

            (
                "Docker Overview".to_string(),
                format!("{ps}\n\n{images}"),
                "Docker overview loaded".to_string(),
            )
        }
        OpsInspectTarget::RbacMatrix { subject } => {
            let mut args = vec![
                "auth".to_string(),
                "can-i".to_string(),
                "--list".to_string(),
            ];
            if let NamespaceScope::Named(namespace) = namespace_scope {
                args.push("-n".to_string());
                args.push(namespace.clone());
            }
            if let Some(subject) = subject.as_ref() {
                args.push("--as".to_string());
                args.push(subject.clone());
            }

            let title = match subject.as_ref() {
                Some(subject) => format!("RBAC Matrix {}", subject),
                None => "RBAC Matrix".to_string(),
            };

            match run_external_readonly("kubectl", &args, 8).await {
                Ok(output) => (
                    title,
                    bounded_output(&output, 260, 220),
                    "RBAC matrix loaded".to_string(),
                ),
                Err(error) => (title, error, "RBAC matrix failed".to_string()),
            }
        }
        OpsInspectTarget::WhoCan {
            verb,
            resource,
            namespace,
        } => {
            let mut args = vec![verb.clone(), resource.clone()];
            if let Some(namespace) = namespace.as_ref().or_else(|| match namespace_scope {
                NamespaceScope::Named(namespace) => Some(namespace),
                NamespaceScope::All => None,
            }) {
                args.push("--namespace".to_string());
                args.push(namespace.clone());
            }

            let title = format!("WhoCan {} {}", verb, resource);
            match run_external_readonly("kubectl-who-can", &args, 12).await {
                Ok(output) => (
                    title,
                    bounded_output(&output, 260, 220),
                    "who-can lookup loaded".to_string(),
                ),
                Err(primary_error) => {
                    let mut fallback = vec!["who-can".to_string()];
                    fallback.extend(args.clone());
                    match run_external_readonly("kubectl", &fallback, 12).await {
                        Ok(output) => (
                            title,
                            bounded_output(&output, 260, 220),
                            "who-can lookup loaded".to_string(),
                        ),
                        Err(fallback_error) => (
                            title,
                            format!(
                                "{}\n\nfallback kubectl who-can failed:\n{}",
                                primary_error, fallback_error
                            ),
                            "who-can lookup failed".to_string(),
                        ),
                    }
                }
            }
        }
        OpsInspectTarget::OpenShiftProjects => {
            let current = match run_external_readonly("oc", &["project".to_string()], 6).await {
                Ok(output) => format!("current\n{}", bounded_output(&output, 18, 220)),
                Err(error) => format!("current\n{error}"),
            };

            let projects = match run_external_readonly("oc", &["projects".to_string()], 6).await {
                Ok(output) => format!("projects\n{}", bounded_output(&output, 160, 220)),
                Err(error) => format!("projects\n{error}"),
            };

            (
                "OpenShift Projects".to_string(),
                format!("{current}\n\n{projects}"),
                "OpenShift project inventory loaded".to_string(),
            )
        }
        OpsInspectTarget::KustomizeBuild { path } => {
            let args = vec!["build".to_string(), path.clone()];
            match run_external_readonly("kustomize", &args, 8).await {
                Ok(output) => (
                    format!("Kustomize Build {}", path),
                    bounded_output(&output, 240, 220),
                    format!("Kustomize build preview loaded: {path}"),
                ),
                Err(error) => (
                    format!("Kustomize Build {}", path),
                    error,
                    format!("Kustomize build preview failed: {path}"),
                ),
            }
        }
        OpsInspectTarget::GitCatalog => {
            let root = repo_cache_root();
            let mut repos = discover_cached_repos(&root);
            repos.sort();
            repos.dedup();

            let mut lines = vec![
                format!("cache {}", root.display()),
                String::new(),
                "commands".to_string(),
                "- :git fetch <url-or-repo> [ref]".to_string(),
                "- :git files <url-or-repo> [path]".to_string(),
                "- :git show <url-or-repo> <path>".to_string(),
                "- :git export <url-or-repo> <source> [destination]".to_string(),
                "- :git apply <url-or-repo> <path>".to_string(),
                String::new(),
                "cached repos".to_string(),
            ];
            if repos.is_empty() {
                lines.push("-".to_string());
            } else {
                lines.extend(repos.into_iter().map(|repo| format!("- {repo}")));
            }

            (
                "Git Repo Toolkit".to_string(),
                lines.join("\n"),
                "Git repo toolkit opened".to_string(),
            )
        }
        OpsInspectTarget::GitFetch { repo, reference } => {
            match ensure_repo_checkout(&repo, reference.as_deref()).await {
                Ok(summary) => {
                    let title = format!("Git Fetch {}", summary.slug);
                    let mut lines = vec![
                        format!("repo {}", summary.slug),
                        format!("path {}", summary.path.display()),
                        format!("status {}", summary.status),
                    ];
                    if let Some(reference) = summary.reference {
                        lines.push(format!("ref {reference}"));
                    }
                    if !summary.output.is_empty() {
                        lines.push(String::new());
                        lines.push(bounded_output(&summary.output, 160, 220));
                    }
                    (
                        title,
                        lines.join("\n"),
                        format!("Repository synced: {}", summary.slug),
                    )
                }
                Err(error) => (
                    "Git Fetch".to_string(),
                    error.clone(),
                    format!("Repository sync failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::GitFiles { repo, path } => {
            match ensure_repo_checkout(&repo, None).await {
                Ok(summary) => {
                    let mut args = vec![
                        "-C".to_string(),
                        summary.path.display().to_string(),
                        "ls-tree".to_string(),
                        "-r".to_string(),
                        "--name-only".to_string(),
                        "HEAD".to_string(),
                    ];
                    if let Some(path) = path.as_ref() {
                        args.push(path.clone());
                    }
                    match run_external_readonly("git", &args, 10).await {
                        Ok(output) => {
                            let list = bounded_output(&output, 260, 220);
                            (
                                format!("Git Files {}", summary.slug),
                                format!(
                                    "repo {}\npath {}\n\n{}",
                                    summary.slug,
                                    summary.path.display(),
                                    list
                                ),
                                format!("Repo files listed: {}", summary.slug),
                            )
                        }
                        Err(error) => (
                            format!("Git Files {}", summary.slug),
                            error.clone(),
                            format!("Repo file listing failed: {error}"),
                        ),
                    }
                }
                Err(error) => (
                    "Git Files".to_string(),
                    error.clone(),
                    format!("Repo file listing failed: {error}"),
                ),
            }
        }
        OpsInspectTarget::GitShow { repo, path } => match ensure_repo_checkout(&repo, None).await {
            Ok(summary) => {
                let spec = format!("HEAD:{path}");
                let args = vec![
                    "-C".to_string(),
                    summary.path.display().to_string(),
                    "--no-pager".to_string(),
                    "show".to_string(),
                    spec,
                ];
                match run_external_readonly("git", &args, 12).await {
                    Ok(output) => (
                        format!("Git Show {} {}", summary.slug, path),
                        bounded_output(&output, 320, 220),
                        format!("Repo file loaded: {path}"),
                    ),
                    Err(error) => (
                        format!("Git Show {} {}", summary.slug, path),
                        error.clone(),
                        format!("Repo file load failed: {error}"),
                    ),
                }
            }
            Err(error) => (
                "Git Show".to_string(),
                error.clone(),
                format!("Repo file load failed: {error}"),
            ),
        },
        OpsInspectTarget::GitExport {
            repo,
            source,
            destination,
        } => match ensure_repo_checkout(&repo, None).await {
            Ok(summary) => {
                let source_path = summary.path.join(source.trim_start_matches('/'));
                let destination_path = PathBuf::from(&destination);
                match copy_repo_path(&source_path, &destination_path) {
                    Ok(copied) => (
                        format!("Git Export {}", summary.slug),
                        [
                            format!("repo {}", summary.slug),
                            format!("from {}", source_path.display()),
                            format!("to {}", destination_path.display()),
                            format!("copied {}", copied),
                        ]
                        .join("\n"),
                        format!("Repo content exported to {}", destination_path.display()),
                    ),
                    Err(error) => (
                        format!("Git Export {}", summary.slug),
                        error.clone(),
                        format!("Repo export failed: {error}"),
                    ),
                }
            }
            Err(error) => (
                "Git Export".to_string(),
                error.clone(),
                format!("Repo export failed: {error}"),
            ),
        },
        OpsInspectTarget::GitApply { repo, path } => {
            match ensure_repo_checkout(&repo, None).await {
                Ok(summary) => {
                    let manifest_path = summary.path.join(path.trim_start_matches('/'));
                    if !manifest_path.exists() {
                        let error =
                            format!("manifest path does not exist: {}", manifest_path.display());
                        (
                            format!("Git Apply {}", summary.slug),
                            error.clone(),
                            format!("Repo apply failed: {error}"),
                        )
                    } else {
                        let mut args = vec![
                            "apply".to_string(),
                            "-f".to_string(),
                            manifest_path.display().to_string(),
                        ];
                        if let NamespaceScope::Named(namespace) = namespace_scope {
                            args.push("-n".to_string());
                            args.push(namespace.clone());
                        }

                        match run_external_readonly("kubectl", &args, 20).await {
                            Ok(output) => (
                                format!("Git Apply {}", summary.slug),
                                bounded_output(&output, 240, 220),
                                format!("Applied manifests from {}", manifest_path.display()),
                            ),
                            Err(error) => (
                                format!("Git Apply {}", summary.slug),
                                error.clone(),
                                format!("Repo apply failed: {error}"),
                            ),
                        }
                    }
                }
                Err(error) => (
                    "Git Apply".to_string(),
                    error.clone(),
                    format!("Repo apply failed: {error}"),
                ),
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RepoCheckoutSummary {
    slug: String,
    path: PathBuf,
    status: String,
    reference: Option<String>,
    output: String,
}

fn repo_cache_root() -> PathBuf {
    if let Ok(path) = std::env::var("ORCA_REPO_CACHE")
        && !path.trim().is_empty()
    {
        return PathBuf::from(path);
    }
    PathBuf::from(".manifests").join("repos")
}

fn discover_cached_repos(root: &Path) -> Vec<String> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if !path.is_dir() || !path.join(".git").exists() {
                return None;
            }
            Some(entry.file_name().to_string_lossy().to_string())
        })
        .collect::<Vec<_>>()
}

fn looks_like_repo_url(repo: &str) -> bool {
    let repo = repo.trim();
    repo.starts_with("http://")
        || repo.starts_with("https://")
        || repo.starts_with("ssh://")
        || repo.starts_with("git@")
        || repo.ends_with(".git")
}

fn repo_slug_from_locator(repo: &str) -> String {
    let trimmed = repo.trim();
    let segment = trimmed
        .rsplit(['/', ':'])
        .find(|segment| !segment.is_empty())
        .unwrap_or(trimmed)
        .trim_end_matches(".git");
    let mut slug = segment
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    while slug.contains("--") {
        slug = slug.replace("--", "-");
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "repo".to_string()
    } else {
        slug
    }
}

async fn ensure_repo_checkout(
    repo: &str,
    reference: Option<&str>,
) -> std::result::Result<RepoCheckoutSummary, String> {
    let repo = repo.trim();
    if repo.is_empty() {
        return Err("repository locator is empty".to_string());
    }

    let root = repo_cache_root();
    fs::create_dir_all(&root)
        .map_err(|error| format!("failed to create repo cache {}: {error}", root.display()))?;

    let is_url = looks_like_repo_url(repo);
    let direct_path = PathBuf::from(repo);
    let (slug, path) = if is_url {
        let slug = repo_slug_from_locator(repo);
        (slug.clone(), root.join(slug))
    } else if direct_path.exists() {
        let slug = direct_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(repo_slug_from_locator)
            .unwrap_or_else(|| "repo".to_string());
        (slug, direct_path)
    } else {
        let slug = repo_slug_from_locator(repo);
        (slug.clone(), root.join(slug))
    };

    let git_dir = path.join(".git");
    let (mut status, mut output_lines) = if is_url {
        if git_dir.exists() {
            let set_origin_args = vec![
                "-C".to_string(),
                path.display().to_string(),
                "remote".to_string(),
                "set-url".to_string(),
                "origin".to_string(),
                repo.to_string(),
            ];
            let _ = run_external_readonly("git", &set_origin_args, 6).await;
            let fetch_args = vec![
                "-C".to_string(),
                path.display().to_string(),
                "fetch".to_string(),
                "--all".to_string(),
                "--prune".to_string(),
            ];
            let output = run_external_readonly("git", &fetch_args, 15).await?;
            let mut lines = Vec::new();
            if !output.trim().is_empty() {
                lines.push(output);
            }
            ("updated".to_string(), lines)
        } else {
            let clone_args = vec![
                "clone".to_string(),
                "--depth=1".to_string(),
                repo.to_string(),
                path.display().to_string(),
            ];
            let output = run_external_readonly("git", &clone_args, 30).await?;
            let mut lines = Vec::new();
            if !output.trim().is_empty() {
                lines.push(output);
            }
            ("cloned".to_string(), lines)
        }
    } else if git_dir.exists() {
        ("opened".to_string(), Vec::new())
    } else {
        return Err(format!(
            "repo '{}' is not cached. Run :git fetch <url> first",
            repo
        ));
    };

    if let Some(reference) = reference {
        let checkout_args = vec![
            "-C".to_string(),
            path.display().to_string(),
            "checkout".to_string(),
            reference.to_string(),
        ];
        match run_external_readonly("git", &checkout_args, 12).await {
            Ok(output) => {
                if !output.trim().is_empty() {
                    output_lines.push(output);
                }
            }
            Err(error) => return Err(error),
        }
        status = format!("{status} + ref");
    }

    let output = output_lines.join("\n");
    Ok(RepoCheckoutSummary {
        slug,
        path,
        status,
        reference: reference.map(str::to_string),
        output,
    })
}

fn copy_repo_path(source: &Path, destination: &Path) -> std::result::Result<usize, String> {
    if !source.exists() {
        return Err(format!("source path does not exist: {}", source.display()));
    }
    if source.is_file() {
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create destination parent {}: {error}",
                parent.display()
            )
        })?;
        fs::copy(source, destination).map_err(|error| {
            format!(
                "failed to copy file {} -> {}: {error}",
                source.display(),
                destination.display()
            )
        })?;
        return Ok(1);
    }

    if !source.is_dir() {
        return Err(format!(
            "source path is not a file or directory: {}",
            source.display()
        ));
    }

    let mut copied = 0usize;
    let mut stack = vec![(source.to_path_buf(), destination.to_path_buf())];
    while let Some((src_dir, dst_dir)) = stack.pop() {
        fs::create_dir_all(&dst_dir).map_err(|error| {
            format!(
                "failed to create destination directory {}: {error}",
                dst_dir.display()
            )
        })?;
        let entries = fs::read_dir(&src_dir)
            .map_err(|error| format!("failed to read {}: {error}", src_dir.display()))?;
        for entry in entries.flatten() {
            let src_path = entry.path();
            let dst_path = dst_dir.join(entry.file_name());
            if src_path.is_dir() {
                stack.push((src_path, dst_path));
            } else if src_path.is_file() {
                if let Some(parent) = dst_path.parent() {
                    fs::create_dir_all(parent).map_err(|error| {
                        format!(
                            "failed to create destination parent {}: {error}",
                            parent.display()
                        )
                    })?;
                }
                fs::copy(&src_path, &dst_path).map_err(|error| {
                    format!(
                        "failed to copy file {} -> {}: {error}",
                        src_path.display(),
                        dst_path.display()
                    )
                })?;
                copied = copied.saturating_add(1);
            }
        }
    }
    Ok(copied)
}

async fn run_external_readonly(
    program: &str,
    args: &[String],
    timeout_secs: u64,
) -> std::result::Result<String, String> {
    let mut cmd = TokioCommand::new(program);
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());

    let output = timeout(Duration::from_secs(timeout_secs), cmd.output())
        .await
        .map_err(|_| format!("{program} timed out after {timeout_secs}s"))?
        .map_err(|error| format!("{program}: {error}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let rendered = if stdout.is_empty() {
        stderr.clone()
    } else if stderr.is_empty() {
        stdout.clone()
    } else {
        format!("{stdout}\n\nstderr:\n{stderr}")
    };

    if output.status.success() {
        Ok(rendered)
    } else if rendered.is_empty() {
        Err(format!("{program} exited with {}", output.status))
    } else {
        Err(format!(
            "{program} failed:\n{}",
            bounded_output(&rendered, 80, 220)
        ))
    }
}

async fn run_external_json(
    program: &str,
    args: &[String],
    timeout_secs: u64,
) -> std::result::Result<Value, String> {
    let mut cmd = TokioCommand::new(program);
    cmd.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());

    let output = timeout(Duration::from_secs(timeout_secs), cmd.output())
        .await
        .map_err(|_| format!("{program} timed out after {timeout_secs}s"))?
        .map_err(|error| format!("{program}: {error}"))?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();

    if !output.status.success() {
        let rendered = if stdout.is_empty() {
            stderr
        } else if stderr.is_empty() {
            stdout
        } else {
            format!("{stdout}\n\nstderr:\n{stderr}")
        };
        if rendered.is_empty() {
            return Err(format!("{program} exited with {}", output.status));
        }
        return Err(format!(
            "{program} failed:\n{}",
            bounded_output(&rendered, 80, 220)
        ));
    }

    serde_json::from_str::<Value>(&stdout).map_err(|error| {
        format!(
            "{program} returned invalid JSON: {error}\n{}",
            bounded_output(&stdout, 40, 220)
        )
    })
}

async fn run_plugin_command(run: &PluginRun) -> Result<String> {
    let timeout_secs = run.timeout_secs.max(1);
    let attempts = usize::from(run.retries).saturating_add(1);
    let mut failures = Vec::new();

    for attempt in 1..=attempts {
        let mut cmd = TokioCommand::new(&run.program);
        cmd.args(&run.args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = timeout(Duration::from_secs(timeout_secs), cmd.output())
            .await
            .map_err(|_| format!("plugin '{}' timed out after {}s", run.name, timeout_secs))
            .and_then(|result| {
                result.map_err(|error| format!("failed to run plugin '{}': {error}", run.name))
            });

        let mut header = vec![
            format!("plugin {}", run.name),
            format!("command {}", run.program),
            format!(
                "args {}",
                if run.args.is_empty() {
                    "(none)".to_string()
                } else {
                    run.args.join(" ")
                }
            ),
            format!("mutating {}", run.mutating),
            format!("profile timeout:{}s retries:{}", timeout_secs, run.retries),
            format!("attempt {attempt}/{attempts}"),
            String::new(),
        ];

        match output {
            Ok(output) => {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let rendered = if stdout.is_empty() {
                    format!("stderr\n{}", bounded_output(&stderr, 260, 220))
                } else if stderr.is_empty() {
                    format!("stdout\n{}", bounded_output(&stdout, 260, 220))
                } else {
                    format!(
                        "stdout\n{}\n\nstderr\n{}",
                        bounded_output(&stdout, 180, 220),
                        bounded_output(&stderr, 80, 220)
                    )
                };
                header.push(rendered);

                if output.status.success() {
                    return Ok(header.join("\n"));
                }

                let mut body = header.join("\n");
                body.push_str(&format!("\n\nexit {}", output.status));
                failures.push(body);
            }
            Err(error) => failures.push(error),
        }
    }

    Err(anyhow::anyhow!(
        "plugin '{}' failed after {} attempt(s)\n\n{}",
        run.name,
        attempts,
        failures.join("\n\n")
    ))
}

fn bounded_output(input: &str, max_lines: usize, max_line_chars: usize) -> String {
    let mut lines = input
        .lines()
        .map(|line| fit_text(line, max_line_chars))
        .collect::<Vec<_>>();

    if lines.is_empty() {
        return "(no output)".to_string();
    }

    if lines.len() > max_lines {
        lines.truncate(max_lines);
        lines.push("…".to_string());
    }

    lines.join("\n")
}

fn discover_ansible_playbooks(root: &str, max_depth: usize, max_files: usize) -> Vec<String> {
    fn walk(
        root: &std::path::Path,
        current: &std::path::Path,
        depth: usize,
        max_depth: usize,
        max_files: usize,
        out: &mut Vec<String>,
    ) {
        if depth > max_depth || out.len() >= max_files {
            return;
        }

        let entries = match std::fs::read_dir(current) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        for entry in entries.flatten() {
            if out.len() >= max_files {
                return;
            }
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || name == "target" || name == ".git" {
                continue;
            }

            if path.is_dir() {
                walk(
                    root,
                    &path,
                    depth.saturating_add(1),
                    max_depth,
                    max_files,
                    out,
                );
                continue;
            }

            let ext = path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if ext != "yml" && ext != "yaml" {
                continue;
            }

            let stem = path
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            let full = path.to_string_lossy().to_ascii_lowercase();
            let looks_like_playbook = stem.contains("playbook")
                || stem == "site"
                || full.contains("/ansible/")
                || full.contains("/playbooks/");
            if !looks_like_playbook {
                continue;
            }

            if let Ok(relative) = path.strip_prefix(root) {
                out.push(relative.display().to_string());
            } else {
                out.push(path.display().to_string());
            }
        }
    }

    let root_path = std::path::Path::new(root);
    let mut found = Vec::new();
    walk(root_path, root_path, 0, max_depth, max_files, &mut found);
    found.sort();
    found.dedup();
    found
}

async fn refresh_tab(app: &mut App, gateway: &KubeGateway, tab: ResourceTab) {
    if matches!(
        tab,
        ResourceTab::ArgoCdApps
            | ResourceTab::ArgoCdResources
            | ResourceTab::ArgoCdProjects
            | ResourceTab::ArgoCdRepos
            | ResourceTab::ArgoCdClusters
            | ResourceTab::ArgoCdAccounts
            | ResourceTab::ArgoCdCerts
            | ResourceTab::ArgoCdGpgKeys
    ) {
        refresh_argocd_tab(app, tab).await;
        return;
    }

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
                match timeout(
                    METRICS_REFRESH_TIMEOUT,
                    gateway.fetch_alert_snapshot(&scope),
                )
                .await
                {
                    Ok(Ok(snapshot)) => app.set_alert_snapshot(snapshot),
                    Ok(Err(error)) => {
                        debug!(
                            "alert snapshot refresh failed for {}: {error:#}",
                            tab.title()
                        )
                    }
                    Err(_) => debug!("alert snapshot refresh timed out for {}", tab.title()),
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

async fn refresh_argocd_tab(app: &mut App, tab: ResourceTab) {
    if let Some(server) = fetch_argocd_server().await {
        app.set_argocd_server(server);
    }

    match tab {
        ResourceTab::ArgoCdApps => match fetch_argocd_apps_table().await {
            Ok(table) => {
                app.set_active_table_data(tab, table);
                if let Some(selected_app) = app.selected_row_name_for(ResourceTab::ArgoCdApps) {
                    app.set_argocd_selected_app(Some(selected_app));
                }
            }
            Err(error) => app.set_active_tab_error(tab, error),
        },
        ResourceTab::ArgoCdResources => {
            let selected_app = app
                .argocd_selected_app()
                .map(str::to_string)
                .or_else(|| app.selected_row_name_for(ResourceTab::ArgoCdApps));
            app.set_argocd_selected_app(selected_app.clone());

            let Some(app_name) = selected_app else {
                let mut table = TableData::default();
                table.set_rows(
                    vec![
                        "Kind".to_string(),
                        "Namespace".to_string(),
                        "Name".to_string(),
                        "Sync".to_string(),
                        "Health".to_string(),
                        "Hook".to_string(),
                        "Wave".to_string(),
                    ],
                    Vec::new(),
                    Local::now(),
                );
                app.set_active_table_data(tab, table);
                app.set_status("Select an Argo CD app first (Enter on ArgoApps)");
                return;
            };

            match fetch_argocd_resources_table(&app_name).await {
                Ok(table) => app.set_active_table_data(tab, table),
                Err(error) => app.set_active_tab_error(tab, error),
            }
        }
        ResourceTab::ArgoCdProjects => match fetch_argocd_projects_table().await {
            Ok(table) => app.set_active_table_data(tab, table),
            Err(error) => app.set_active_tab_error(tab, error),
        },
        ResourceTab::ArgoCdRepos => match fetch_argocd_repos_table().await {
            Ok(table) => app.set_active_table_data(tab, table),
            Err(error) => app.set_active_tab_error(tab, error),
        },
        ResourceTab::ArgoCdClusters => match fetch_argocd_clusters_table().await {
            Ok(table) => app.set_active_table_data(tab, table),
            Err(error) => app.set_active_tab_error(tab, error),
        },
        ResourceTab::ArgoCdAccounts => match fetch_argocd_accounts_table().await {
            Ok(table) => app.set_active_table_data(tab, table),
            Err(error) => app.set_active_tab_error(tab, error),
        },
        ResourceTab::ArgoCdCerts => match fetch_argocd_certs_table().await {
            Ok(table) => app.set_active_table_data(tab, table),
            Err(error) => app.set_active_tab_error(tab, error),
        },
        ResourceTab::ArgoCdGpgKeys => match fetch_argocd_gpg_table().await {
            Ok(table) => app.set_active_table_data(tab, table),
            Err(error) => app.set_active_tab_error(tab, error),
        },
        _ => {}
    }
}

async fn fetch_argocd_server() -> Option<String> {
    let output = run_external_readonly("argocd", &["context".to_string()], 5)
        .await
        .ok()?;
    parse_argocd_context_server(&output)
}

fn parse_argocd_context_server(output: &str) -> Option<String> {
    for line in output.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("CURRENT") {
            continue;
        }
        if let Some(stripped) = trimmed.strip_prefix('*') {
            let parts = stripped.split_whitespace().collect::<Vec<_>>();
            if let Some(server) = parts.last() {
                return Some((*server).to_string());
            }
        }
    }

    output
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("CURRENT"))
        .and_then(|line| line.split_whitespace().last().map(str::to_string))
}

async fn fetch_argocd_apps_table() -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "app".to_string(),
            "list".to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        10,
    )
    .await?;
    let apps = payload
        .as_array()
        .cloned()
        .ok_or_else(|| "argocd app list output is not an array".to_string())?;

    let mut rows = Vec::new();
    for item in apps {
        let name = item
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let project = item
            .pointer("/spec/project")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let dest_namespace = item
            .pointer("/spec/destination/namespace")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let sync = item
            .pointer("/status/sync/status")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let health = item
            .pointer("/status/health/status")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let repo = item
            .pointer("/spec/source/repoURL")
            .and_then(Value::as_str)
            .map(short_repo_label)
            .unwrap_or_else(|| "-".to_string());
        let path = item
            .pointer("/spec/source/path")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();

        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: name.clone(),
            namespace: Some(dest_namespace.clone()),
            columns: vec![name, project, dest_namespace, sync, health, repo, path],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Name".to_string(),
            "Project".to_string(),
            "Namespace".to_string(),
            "Sync".to_string(),
            "Health".to_string(),
            "Repo".to_string(),
            "Path".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

async fn fetch_argocd_resources_table(app_name: &str) -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "app".to_string(),
            "get".to_string(),
            app_name.to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        12,
    )
    .await?;

    let resources = payload
        .pointer("/status/resources")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    #[derive(Debug, Clone)]
    struct ArgoTreeNode {
        kind: String,
        namespace: String,
        name: String,
        sync: String,
        health: String,
        hook: String,
        wave: String,
        parent: Option<String>,
        detail: String,
    }

    fn node_key(kind: &str, namespace: &str, name: &str) -> String {
        format!("{}|{}|{}", namespace.trim(), kind.trim(), name.trim())
    }

    fn owner_key(namespace: &str, owner_kind: &str, owner_name: &str) -> String {
        node_key(owner_kind, namespace, owner_name)
    }

    fn kind_rank(kind: &str) -> u8 {
        match kind.to_ascii_lowercase().as_str() {
            "service" => 1,
            "configmap" => 2,
            "secret" => 3,
            "deployment" => 10,
            "statefulset" => 11,
            "daemonset" => 12,
            "job" => 13,
            "cronjob" => 14,
            "replicaset" => 20,
            "pod" => 30,
            _ => 100,
        }
    }

    fn kind_icon(kind: &str) -> &'static str {
        match kind.to_ascii_lowercase().as_str() {
            "pod" => "󰋊",
            "deployment" => "󰹑",
            "replicaset" => "󰹍",
            "statefulset" => "󰛨",
            "daemonset" => "󰠱",
            "job" => "󰁨",
            "cronjob" => "󰃰",
            "service" => "󰒓",
            "configmap" => "󰈙",
            "secret" => "󰌋",
            "namespace" => "󰉖",
            "node" => "󰣇",
            _ => "󰈔",
        }
    }

    fn tree_kind_label(kind: &str) -> String {
        match kind.to_ascii_lowercase().as_str() {
            "service" => "Service".to_string(),
            "deployment" => "Deployment".to_string(),
            "replicaset" => "ReplicaSe".to_string(),
            "replicationcontroller" => "ReplCtrl".to_string(),
            "statefulset" => "StatefulSe".to_string(),
            "daemonset" => "DaemonSet".to_string(),
            "job" => "Job".to_string(),
            "cronjob" => "CronJob".to_string(),
            "pod" => "Pod".to_string(),
            "configmap" => "CfgMap".to_string(),
            "secret" => "Secret".to_string(),
            "namespace" => "Namespace".to_string(),
            "node" => "Node".to_string(),
            other => other.chars().take(10).collect::<String>(),
        }
    }

    fn rs_health(item: &Value) -> String {
        let replicas = item
            .pointer("/status/replicas")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let ready = item
            .pointer("/status/readyReplicas")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        if replicas == 0 {
            "ScaledDown".to_string()
        } else if ready >= replicas {
            "Healthy".to_string()
        } else {
            "Progressing".to_string()
        }
    }

    fn pod_health(item: &Value) -> String {
        item.pointer("/status/phase")
            .and_then(Value::as_str)
            .unwrap_or("Unknown")
            .to_string()
    }

    fn render_tree_rows(
        key: &str,
        nodes: &HashMap<String, ArgoTreeNode>,
        children: &HashMap<String, Vec<String>>,
        ancestor_last: &[bool],
        is_last: bool,
        rows: &mut Vec<RowData>,
        visited: &mut HashSet<String>,
    ) {
        if !visited.insert(key.to_string()) {
            return;
        }
        let Some(node) = nodes.get(key) else {
            return;
        };

        let depth = ancestor_last.len();
        let prefix = if depth == 0 && node.parent.is_none() {
            "  ".to_string()
        } else {
            format!(
                "{}{}",
                "  ".repeat(depth),
                if is_last { "└─" } else { "├─" }
            )
        };
        let tree_kind = format!(
            "{prefix}{} {}",
            kind_icon(&node.kind),
            tree_kind_label(&node.kind)
        );
        rows.push(RowData {
            name: format!("{}/{}", node.kind, node.name),
            namespace: Some(node.namespace.clone()),
            columns: vec![
                tree_kind,
                node.namespace.clone(),
                node.name.clone(),
                node.sync.clone(),
                node.health.clone(),
                node.hook.clone(),
                node.wave.clone(),
            ],
            detail: node.detail.clone(),
        });

        let branch = children.get(key).cloned().unwrap_or_default();
        if branch.is_empty() {
            return;
        }
        for (index, child_key) in branch.iter().enumerate() {
            let mut next_ancestors = ancestor_last.to_vec();
            next_ancestors.push(is_last);
            render_tree_rows(
                child_key,
                nodes,
                children,
                &next_ancestors,
                index == branch.len().saturating_sub(1),
                rows,
                visited,
            );
        }
    }

    let mut nodes = HashMap::<String, ArgoTreeNode>::new();
    let mut root_keys = Vec::<String>::new();
    let mut tracked_deployments = HashSet::<String>::new();
    let mut tracked_statefulsets = HashSet::<String>::new();
    let mut tracked_daemonsets = HashSet::<String>::new();
    let mut tracked_jobs = HashSet::<String>::new();
    let mut tracked_replicasets = HashSet::<String>::new();

    for item in resources {
        let kind = item
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let namespace = item
            .get("namespace")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let sync = item
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let health = item
            .pointer("/health/status")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let hook = item
            .get("hookType")
            .and_then(Value::as_str)
            .or_else(|| item.get("hook").and_then(Value::as_str))
            .unwrap_or("-")
            .to_string();
        let wave = item
            .get("syncWave")
            .and_then(|value| {
                value
                    .as_i64()
                    .map(|wave| wave.to_string())
                    .or_else(|| value.as_str().map(str::to_string))
            })
            .unwrap_or_else(|| "-".to_string());

        let key = node_key(&kind, &namespace, &name);
        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        nodes.insert(
            key.clone(),
            ArgoTreeNode {
                kind: kind.clone(),
                namespace: namespace.clone(),
                name: name.clone(),
                sync,
                health,
                hook,
                wave,
                parent: None,
                detail,
            },
        );
        root_keys.push(key.clone());

        let set_key = format!("{}|{}", namespace, name);
        match kind.to_ascii_lowercase().as_str() {
            "deployment" => {
                tracked_deployments.insert(set_key);
            }
            "statefulset" => {
                tracked_statefulsets.insert(set_key);
            }
            "daemonset" => {
                tracked_daemonsets.insert(set_key);
            }
            "job" => {
                tracked_jobs.insert(set_key);
            }
            "replicaset" => {
                tracked_replicasets.insert(set_key);
            }
            _ => {}
        }
    }

    let namespaces = nodes
        .values()
        .map(|node| node.namespace.clone())
        .filter(|namespace| !namespace.is_empty() && namespace != "-")
        .collect::<HashSet<_>>();

    for namespace in namespaces {
        let payload = run_external_json(
            "kubectl",
            &[
                "get".to_string(),
                "replicasets,pods".to_string(),
                "-n".to_string(),
                namespace.clone(),
                "-o".to_string(),
                "json".to_string(),
            ],
            10,
        )
        .await;
        let Ok(payload) = payload else {
            continue;
        };
        let items = payload
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        for item in &items {
            let kind = item
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if !kind.eq_ignore_ascii_case("replicaset") {
                continue;
            }
            let name = item
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let owner = item
                .pointer("/metadata/ownerReferences")
                .and_then(Value::as_array)
                .and_then(|owners| owners.first())
                .cloned();
            let Some(owner) = owner else {
                continue;
            };
            let owner_kind = owner.get("kind").and_then(Value::as_str).unwrap_or("");
            let owner_name = owner.get("name").and_then(Value::as_str).unwrap_or("");
            if owner_name.is_empty() || owner_kind.is_empty() {
                continue;
            }

            let owner_set_key = format!("{}|{}", namespace, owner_name);
            let is_supported_parent = (owner_kind.eq_ignore_ascii_case("deployment")
                && tracked_deployments.contains(&owner_set_key))
                || (owner_kind.eq_ignore_ascii_case("statefulset")
                    && tracked_statefulsets.contains(&owner_set_key))
                || (owner_kind.eq_ignore_ascii_case("daemonset")
                    && tracked_daemonsets.contains(&owner_set_key))
                || (owner_kind.eq_ignore_ascii_case("job")
                    && tracked_jobs.contains(&owner_set_key));
            if !is_supported_parent {
                continue;
            }

            let rs_key = node_key("ReplicaSet", &namespace, &name);
            if nodes.contains_key(&rs_key) {
                tracked_replicasets.insert(format!("{}|{}", namespace, name));
                continue;
            }

            let detail = serde_json::to_string_pretty(item).unwrap_or_else(|_| item.to_string());
            nodes.insert(
                rs_key.clone(),
                ArgoTreeNode {
                    kind: "ReplicaSet".to_string(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    sync: "Live".to_string(),
                    health: rs_health(item),
                    hook: "-".to_string(),
                    wave: "-".to_string(),
                    parent: Some(owner_key(&namespace, owner_kind, owner_name)),
                    detail,
                },
            );
            tracked_replicasets.insert(format!("{}|{}", namespace, name));
        }

        for item in items {
            let kind = item
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if !kind.eq_ignore_ascii_case("pod") {
                continue;
            }
            let name = item
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let owner = item
                .pointer("/metadata/ownerReferences")
                .and_then(Value::as_array)
                .and_then(|owners| owners.first())
                .cloned();
            let Some(owner) = owner else {
                continue;
            };
            let owner_kind = owner.get("kind").and_then(Value::as_str).unwrap_or("");
            let owner_name = owner.get("name").and_then(Value::as_str).unwrap_or("");
            if owner_name.is_empty() || owner_kind.is_empty() {
                continue;
            }

            let owner_set_key = format!("{}|{}", namespace, owner_name);
            let parent = if owner_kind.eq_ignore_ascii_case("replicaset")
                && tracked_replicasets.contains(&owner_set_key)
            {
                Some(owner_key(&namespace, "ReplicaSet", owner_name))
            } else if owner_kind.eq_ignore_ascii_case("statefulset")
                && tracked_statefulsets.contains(&owner_set_key)
            {
                Some(owner_key(&namespace, "StatefulSet", owner_name))
            } else if owner_kind.eq_ignore_ascii_case("daemonset")
                && tracked_daemonsets.contains(&owner_set_key)
            {
                Some(owner_key(&namespace, "DaemonSet", owner_name))
            } else if owner_kind.eq_ignore_ascii_case("job")
                && tracked_jobs.contains(&owner_set_key)
            {
                Some(owner_key(&namespace, "Job", owner_name))
            } else {
                None
            };
            let Some(parent) = parent else {
                continue;
            };

            let pod_key = node_key("Pod", &namespace, &name);
            if nodes.contains_key(&pod_key) {
                continue;
            }
            let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
            nodes.insert(
                pod_key,
                ArgoTreeNode {
                    kind: "Pod".to_string(),
                    namespace: namespace.clone(),
                    name: name.clone(),
                    sync: "Live".to_string(),
                    health: pod_health(&item),
                    hook: "-".to_string(),
                    wave: "-".to_string(),
                    parent: Some(parent),
                    detail,
                },
            );
        }
    }

    let mut children = HashMap::<String, Vec<String>>::new();
    for (key, node) in &nodes {
        if let Some(parent) = node.parent.as_ref() {
            children
                .entry(parent.clone())
                .or_default()
                .push(key.clone());
        }
    }
    for values in children.values_mut() {
        values.sort_by(|left, right| {
            let left_node = nodes.get(left);
            let right_node = nodes.get(right);
            match (left_node, right_node) {
                (Some(left), Some(right)) => kind_rank(&left.kind)
                    .cmp(&kind_rank(&right.kind))
                    .then_with(|| left.name.cmp(&right.name)),
                _ => left.cmp(right),
            }
        });
    }

    let mut rows = Vec::new();
    let mut visited = HashSet::<String>::new();
    root_keys.sort_by(|left, right| {
        let left_node = nodes.get(left);
        let right_node = nodes.get(right);
        match (left_node, right_node) {
            (Some(left), Some(right)) => kind_rank(&left.kind)
                .cmp(&kind_rank(&right.kind))
                .then_with(|| left.name.cmp(&right.name)),
            _ => left.cmp(right),
        }
    });
    root_keys.dedup();
    for (index, root_key) in root_keys.iter().enumerate() {
        render_tree_rows(
            root_key,
            &nodes,
            &children,
            &[],
            index == root_keys.len().saturating_sub(1),
            &mut rows,
            &mut visited,
        );
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Tree".to_string(),
            "Namespace".to_string(),
            "Name".to_string(),
            "Sync".to_string(),
            "Health".to_string(),
            "Hook".to_string(),
            "Wave".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

fn kubectl_resource_token_for_kind(kind: &str) -> String {
    match kind.to_ascii_lowercase().as_str() {
        "pod" => "pod".to_string(),
        "deployment" => "deployment".to_string(),
        "replicaset" => "replicaset".to_string(),
        "statefulset" => "statefulset".to_string(),
        "daemonset" => "daemonset".to_string(),
        "job" => "job".to_string(),
        "cronjob" => "cronjob".to_string(),
        "service" => "service".to_string(),
        "namespace" => "namespace".to_string(),
        "node" => "node".to_string(),
        other => other.to_string(),
    }
}

fn supports_argocd_logs(kind: &str) -> bool {
    matches!(
        kind.to_ascii_lowercase().as_str(),
        "pod"
            | "deployment"
            | "replicaset"
            | "replicationcontroller"
            | "statefulset"
            | "daemonset"
            | "job"
            | "cronjob"
    )
}

#[derive(Debug, Clone)]
struct ArgoResourcePanelSections {
    summary: String,
    events: String,
    logs: String,
    manifest: String,
}

async fn fetch_argocd_resource_panel_sections(
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> std::result::Result<(String, ArgoResourcePanelSections), String> {
    let namespace = namespace
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
        .map(str::to_string);
    let token = kubectl_resource_token_for_kind(kind);

    let mut get_json_args = vec!["get".to_string(), token.clone(), name.to_string()];
    if let Some(namespace) = namespace.as_deref() {
        get_json_args.push("-n".to_string());
        get_json_args.push(namespace.to_string());
    }
    get_json_args.push("-o".to_string());
    get_json_args.push("json".to_string());
    let summary_json = run_external_json("kubectl", &get_json_args, 8).await;

    let summary_block = match summary_json.as_ref() {
        Ok(object) => {
            let created = object
                .pointer("/metadata/creationTimestamp")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let object_namespace = object
                .pointer("/metadata/namespace")
                .and_then(Value::as_str)
                .or(namespace.as_deref())
                .unwrap_or("-");
            let labels = object
                .pointer("/metadata/labels")
                .and_then(Value::as_object)
                .map(|labels| labels.len().to_string())
                .unwrap_or_else(|| "0".to_string());
            let uid = object
                .pointer("/metadata/uid")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let readiness = match kind.to_ascii_lowercase().as_str() {
                "deployment" | "replicaset" | "statefulset" | "daemonset" => {
                    let ready = object
                        .pointer("/status/readyReplicas")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let replicas = object
                        .pointer("/status/replicas")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    format!("{ready}/{replicas}")
                }
                "pod" => {
                    let ready = object
                        .pointer("/status/containerStatuses")
                        .and_then(Value::as_array)
                        .map(|statuses| {
                            let total = statuses.len();
                            let ready = statuses
                                .iter()
                                .filter(|entry| {
                                    entry.get("ready").and_then(Value::as_bool).unwrap_or(false)
                                })
                                .count();
                            format!("{ready}/{total}")
                        })
                        .unwrap_or_else(|| "-".to_string());
                    let phase = object
                        .pointer("/status/phase")
                        .and_then(Value::as_str)
                        .unwrap_or("-");
                    format!("{ready} ({phase})")
                }
                _ => "-".to_string(),
            };
            format!(
                "kind: {kind}\nname: {name}\nnamespace: {object_namespace}\ncreated: {created}\nready: {readiness}\nlabels: {labels}\nuid: {uid}"
            )
        }
        Err(error) => format!("Summary unavailable: {error}"),
    };

    let mut event_args = vec![
        "get".to_string(),
        "events".to_string(),
        "--field-selector".to_string(),
        format!("involvedObject.kind={kind},involvedObject.name={name}"),
        "--sort-by=.lastTimestamp".to_string(),
        "-o".to_string(),
        "custom-columns=LAST:.lastTimestamp,TYPE:.type,REASON:.reason,MESSAGE:.message".to_string(),
    ];
    if let Some(namespace) = namespace.as_deref() {
        event_args.insert(2, "-n".to_string());
        event_args.insert(3, namespace.to_string());
    }
    let events_block = run_external_readonly("kubectl", &event_args, 8)
        .await
        .map(|output| bounded_output(&output, 40, 220))
        .unwrap_or_else(|error| format!("Events unavailable: {error}"));

    let logs_block = if supports_argocd_logs(kind) {
        if let Some(namespace) = namespace.as_deref() {
            let mut logs_args = vec![
                "logs".to_string(),
                format!("{}/{}", token, name),
                "--all-containers=true".to_string(),
                "--tail=80".to_string(),
            ];
            logs_args.push("-n".to_string());
            logs_args.push(namespace.to_string());
            run_external_readonly("kubectl", &logs_args, 10)
                .await
                .map(|output| bounded_output(&output, 80, 220))
                .unwrap_or_else(|error| format!("Logs unavailable: {error}"))
        } else {
            "Logs unavailable: namespace is required".to_string()
        }
    } else {
        "Logs unavailable for this kind".to_string()
    };

    let mut manifest_args = vec!["get".to_string(), token.clone(), name.to_string()];
    if let Some(namespace) = namespace.as_deref() {
        manifest_args.push("-n".to_string());
        manifest_args.push(namespace.to_string());
    }
    manifest_args.push("-o".to_string());
    manifest_args.push("yaml".to_string());
    let manifest_block = run_external_readonly("kubectl", &manifest_args, 10)
        .await
        .map(|output| bounded_output(&output, 240, 220))
        .unwrap_or_else(|error| format!("Manifest unavailable: {error}"));

    let title = match namespace.as_deref() {
        Some(namespace) => format!("Argo {kind} {namespace}/{name}"),
        None => format!("Argo {kind} {name}"),
    };
    Ok((
        title,
        ArgoResourcePanelSections {
            summary: summary_block,
            events: events_block,
            logs: logs_block,
            manifest: manifest_block,
        },
    ))
}

async fn fetch_argocd_resource_panel(
    kind: &str,
    namespace: Option<&str>,
    name: &str,
) -> std::result::Result<(String, String), String> {
    let (title, sections) = fetch_argocd_resource_panel_sections(kind, namespace, name).await?;
    let panel = format!(
        "SUMMARY\n{}\n\nEVENTS\n{}\n\nLOGS\n{}\n\nLIVE MANIFEST\n{}",
        sections.summary, sections.events, sections.logs, sections.manifest
    );
    Ok((title, panel))
}

async fn fetch_argocd_projects_table() -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "proj".to_string(),
            "list".to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        12,
    )
    .await?;
    let projects = payload
        .as_array()
        .cloned()
        .ok_or_else(|| "argocd proj list output is not an array".to_string())?;

    let mut rows = Vec::new();
    for item in projects {
        let name = item
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let namespace = item
            .pointer("/metadata/namespace")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let destinations = item
            .pointer("/spec/destinations")
            .and_then(Value::as_array)
            .map(|items| items.len().to_string())
            .unwrap_or_else(|| "0".to_string());
        let repos = item
            .pointer("/spec/sourceRepos")
            .and_then(Value::as_array)
            .map(|items| items.len().to_string())
            .unwrap_or_else(|| "0".to_string());
        let cluster_whitelist = item
            .pointer("/spec/clusterResourceWhitelist")
            .and_then(Value::as_array)
            .map(|items| items.len().to_string())
            .unwrap_or_else(|| "0".to_string());
        let namespace_whitelist = item
            .pointer("/spec/namespaceResourceWhitelist")
            .and_then(Value::as_array)
            .map(|items| items.len().to_string())
            .unwrap_or_else(|| "0".to_string());

        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: name.clone(),
            namespace: Some(namespace.clone()),
            columns: vec![
                name,
                namespace,
                destinations,
                repos,
                cluster_whitelist,
                namespace_whitelist,
            ],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Name".to_string(),
            "Namespace".to_string(),
            "Destinations".to_string(),
            "Repos".to_string(),
            "ClusterWL".to_string(),
            "NamespaceWL".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

async fn fetch_argocd_repos_table() -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "repo".to_string(),
            "list".to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        10,
    )
    .await?;
    let repos = payload
        .as_array()
        .cloned()
        .ok_or_else(|| "argocd repo list output is not an array".to_string())?;

    let mut rows = Vec::new();
    for item in repos {
        let repo = item
            .get("repo")
            .and_then(Value::as_str)
            .or_else(|| item.get("url").and_then(Value::as_str))
            .unwrap_or("-")
            .to_string();
        let typ = item
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let project = item
            .get("project")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let insecure = item
            .get("insecure")
            .and_then(Value::as_bool)
            .map(|value| if value { "yes" } else { "no" })
            .unwrap_or("no")
            .to_string();
        let oci = item
            .get("enableOCI")
            .and_then(Value::as_bool)
            .map(|value| if value { "yes" } else { "no" })
            .unwrap_or("no")
            .to_string();

        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: short_repo_label(&repo),
            namespace: None,
            columns: vec![repo, typ, name, project, insecure, oci],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Repo".to_string(),
            "Type".to_string(),
            "Name".to_string(),
            "Project".to_string(),
            "Insecure".to_string(),
            "OCI".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

async fn fetch_argocd_clusters_table() -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "cluster".to_string(),
            "list".to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        12,
    )
    .await?;
    let clusters = payload
        .as_array()
        .cloned()
        .ok_or_else(|| "argocd cluster list output is not an array".to_string())?;

    let mut rows = Vec::new();
    for item in clusters {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let server = item
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let status = item
            .pointer("/connectionState/status")
            .and_then(Value::as_str)
            .or_else(|| {
                item.pointer("/info/connectionState/status")
                    .and_then(Value::as_str)
            })
            .unwrap_or("-")
            .to_string();
        let version = item
            .get("serverVersion")
            .and_then(Value::as_str)
            .or_else(|| item.pointer("/info/serverVersion").and_then(Value::as_str))
            .unwrap_or("-")
            .to_string();
        let applications = item
            .pointer("/info/applicationsCount")
            .and_then(Value::as_u64)
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string());

        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: name.clone(),
            namespace: None,
            columns: vec![name, server, status, version, applications],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Name".to_string(),
            "Server".to_string(),
            "Status".to_string(),
            "Version".to_string(),
            "Apps".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

async fn fetch_argocd_accounts_table() -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "account".to_string(),
            "list".to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        10,
    )
    .await?;
    let accounts = payload
        .as_array()
        .cloned()
        .ok_or_else(|| "argocd account list output is not an array".to_string())?;

    let mut rows = Vec::new();
    for item in accounts {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let enabled = item
            .get("enabled")
            .and_then(Value::as_bool)
            .map(|value| if value { "yes" } else { "no" })
            .unwrap_or("-")
            .to_string();
        let capabilities = item
            .get("capabilities")
            .and_then(Value::as_array)
            .map(|caps| {
                caps.iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_else(|| "-".to_string());
        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: name.clone(),
            namespace: None,
            columns: vec![name, enabled, capabilities],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Name".to_string(),
            "Enabled".to_string(),
            "Capabilities".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

async fn fetch_argocd_certs_table() -> std::result::Result<TableData, String> {
    let mut certs = Vec::<Value>::new();
    let mut errors = Vec::<String>::new();
    for cert_type in ["https", "ssh"] {
        let payload = run_external_json(
            "argocd",
            &[
                "cert".to_string(),
                "list".to_string(),
                "--cert-type".to_string(),
                cert_type.to_string(),
                "-o".to_string(),
                "json".to_string(),
            ],
            10,
        )
        .await;
        match payload {
            Ok(payload) => {
                if let Some(entries) = payload.as_array() {
                    certs.extend(entries.iter().cloned());
                } else {
                    errors.push(format!(
                        "argocd cert list ({cert_type}) returned non-array JSON"
                    ));
                }
            }
            Err(error) => errors.push(format!("argocd cert list ({cert_type}) failed: {error}")),
        }
    }

    if certs.is_empty() && !errors.is_empty() {
        return Err(errors.join("\n"));
    }

    let mut rows = Vec::new();
    for item in certs {
        let server = item
            .get("serverName")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let cert_type = item
            .get("certType")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let sub_type = item
            .get("certSubType")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let fingerprint = item
            .get("certInfo")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();

        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: format!("{server}:{sub_type}"),
            namespace: None,
            columns: vec![server, cert_type, sub_type, fingerprint],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "Server".to_string(),
            "Type".to_string(),
            "SubType".to_string(),
            "Fingerprint".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

async fn fetch_argocd_gpg_table() -> std::result::Result<TableData, String> {
    let payload = run_external_json(
        "argocd",
        &[
            "gpg".to_string(),
            "list".to_string(),
            "-o".to_string(),
            "json".to_string(),
        ],
        10,
    )
    .await?;
    let keys = payload
        .as_array()
        .cloned()
        .ok_or_else(|| "argocd gpg list output is not an array".to_string())?;

    let mut rows = Vec::new();
    for item in keys {
        let fingerprint = item
            .get("fingerprint")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_string();
        let key_id = item
            .get("keyID")
            .and_then(Value::as_str)
            .or_else(|| item.get("keyId").and_then(Value::as_str))
            .or_else(|| item.get("id").and_then(Value::as_str))
            .unwrap_or_else(|| {
                if fingerprint.len() > 16 {
                    &fingerprint[fingerprint.len() - 16..]
                } else {
                    "-"
                }
            })
            .to_string();
        let users = item
            .get("uids")
            .and_then(Value::as_array)
            .map(|uids| {
                uids.iter()
                    .filter_map(|entry| {
                        if let Some(value) = entry.as_str() {
                            return Some(value.to_string());
                        }
                        if let Some(name) = entry.get("name").and_then(Value::as_str) {
                            return Some(name.to_string());
                        }
                        entry.get("uid").and_then(Value::as_str).map(str::to_string)
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_else(|| "-".to_string());

        let detail = serde_json::to_string_pretty(&item).unwrap_or_else(|_| item.to_string());
        rows.push(RowData {
            name: key_id.clone(),
            namespace: None,
            columns: vec![key_id, fingerprint, users],
            detail,
        });
    }

    let mut table = TableData::default();
    table.set_rows(
        vec![
            "KeyID".to_string(),
            "Fingerprint".to_string(),
            "UIDs".to_string(),
        ],
        rows,
        Local::now(),
    );
    Ok(table)
}

fn short_repo_label(repo: &str) -> String {
    let trimmed = repo.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return "-".to_string();
    }
    let without_scheme = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);
    let parts = without_scheme.split('/').collect::<Vec<_>>();
    if parts.len() >= 2 {
        format!("{}/{}", parts[parts.len() - 2], parts[parts.len() - 1])
    } else {
        without_scheme.to_string()
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

struct StartedEmbeddedShell {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    writer: Box<dyn Write + Send>,
    reader: Box<dyn Read + Send>,
}

fn start_embedded_kubectl_shell(
    namespace: &str,
    pod_name: &str,
    container: Option<&str>,
    shell: &str,
) -> Result<StartedEmbeddedShell> {
    const AUTO_SHELL_BOOTSTRAP: &str = "export TERM=${TERM:-xterm-256color}; \
if command -v bash >/dev/null 2>&1; then exec bash -il; \
elif command -v zsh >/dev/null 2>&1; then exec zsh -il; \
elif command -v ash >/dev/null 2>&1; then exec ash -i; \
elif command -v sh >/dev/null 2>&1; then exec sh -i; \
else exec /bin/sh -i; fi";

    let pty_system = native_pty_system();
    let pty_pair = pty_system
        .openpty(PtySize {
            rows: 48,
            cols: 180,
            pixel_width: 0,
            pixel_height: 0,
        })
        .context("failed to allocate pseudo-tty for embedded shell")?;

    let mut cmd = PtyCommandBuilder::new("kubectl");
    cmd.env("TERM", "xterm-256color");
    cmd.arg("exec");
    cmd.arg("-i");
    cmd.arg("-t");
    cmd.arg("-n");
    cmd.arg(namespace);
    cmd.arg(pod_name);
    if let Some(container) = container {
        cmd.arg("-c");
        cmd.arg(container);
    }
    cmd.arg("--");
    if shell.eq_ignore_ascii_case("auto") {
        cmd.arg("sh");
        cmd.arg("-lc");
        cmd.arg(AUTO_SHELL_BOOTSTRAP);
    } else {
        cmd.arg(shell);
        cmd.arg("-i");
    }

    let child = pty_pair
        .slave
        .spawn_command(cmd)
        .with_context(|| format!("failed to start embedded shell for {namespace}/{pod_name}"))?;

    let reader = pty_pair
        .master
        .try_clone_reader()
        .context("failed to capture embedded shell reader")?;
    let writer = pty_pair
        .master
        .take_writer()
        .context("failed to capture embedded shell writer")?;

    Ok(StartedEmbeddedShell {
        child,
        writer,
        reader,
    })
}

fn spawn_shell_reader(
    mut reader: Box<dyn Read + Send>,
    tx: mpsc::UnboundedSender<ShellOutputEvent>,
) {
    std::thread::spawn(move || {
        let mut parser = vt100::Parser::new(200, 240, 4_000);
        let mut buffer = vec![0u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    parser.process(&buffer[..read]);
                    let snapshot = render_shell_snapshot(parser.screen());
                    let application_cursor = parser.screen().application_cursor();
                    let _ = tx.send(ShellOutputEvent {
                        snapshot,
                        application_cursor,
                    });
                }
                Err(error) => {
                    let _ = tx.send(ShellOutputEvent {
                        snapshot: format!("[orca] shell stream error: {error}"),
                        application_cursor: false,
                    });
                    break;
                }
            }
        }
    });
}

fn render_shell_snapshot(screen: &vt100::Screen) -> String {
    let (rows, cols) = screen.size();
    let mut lines = screen
        .rows(0, cols)
        .take(rows as usize)
        .collect::<Vec<String>>();

    let (cursor_row, cursor_col) = screen.cursor_position();
    if let Some(line) = lines.get_mut(cursor_row as usize) {
        let mut chars = line.chars().collect::<Vec<char>>();
        let cursor_index = cursor_col as usize;
        if cursor_index >= chars.len() {
            chars.resize(cursor_index, ' ');
            chars.push('█');
        } else {
            chars[cursor_index] = '█';
        }
        *line = chars.into_iter().collect();
    }

    while lines.last().is_some_and(|line| line.trim_end().is_empty()) {
        lines.pop();
    }

    lines.join("\n")
}

#[cfg(test)]
mod shell_snapshot_tests {
    use super::render_shell_snapshot;

    #[test]
    fn renders_block_cursor_without_raw_escape_bytes() {
        let mut parser = vt100::Parser::new(8, 40, 32);
        parser.process(b"\x1b[32mhello\x1b[0m");
        let rendered = render_shell_snapshot(parser.screen());
        assert!(rendered.contains("hello"));
        assert!(rendered.contains('█'));
        assert!(!rendered.contains("\x1b"));
    }

    #[test]
    fn trims_trailing_blank_lines() {
        let mut parser = vt100::Parser::new(8, 40, 32);
        parser.process(b"line1\nline2");
        let rendered = render_shell_snapshot(parser.screen());
        assert!(rendered.contains("line1"));
        assert!(rendered.contains("line2"));
        assert!(!rendered.ends_with('\n'));
    }
}

fn write_embedded_shell_bytes(writer: &mut Option<Box<dyn Write + Send>>, bytes: &[u8]) -> bool {
    let Some(writer) = writer.as_mut() else {
        return false;
    };

    if writer.write_all(bytes).is_err() {
        return false;
    }
    let _ = writer.flush();
    true
}

fn forward_key_to_embedded_shell(
    key: KeyEvent,
    writer: &mut Option<Box<dyn Write + Send>>,
    application_cursor: bool,
) -> bool {
    let control = key.modifiers.contains(KeyModifiers::CONTROL);
    let alt = key.modifiers.contains(KeyModifiers::ALT);

    match key.code {
        KeyCode::Char(c) => {
            if control {
                let lower = c.to_ascii_lowercase();
                if lower.is_ascii_alphabetic() {
                    let byte = (lower as u8).saturating_sub(b'a').saturating_add(1);
                    return write_embedded_shell_bytes(writer, &[byte]);
                }
                if c == ' ' {
                    return write_embedded_shell_bytes(writer, &[0x00]);
                }
                return false;
            }
            let mut utf8 = [0u8; 4];
            let encoded = c.encode_utf8(&mut utf8);
            if alt {
                let mut prefixed = vec![0x1b];
                prefixed.extend_from_slice(encoded.as_bytes());
                write_embedded_shell_bytes(writer, &prefixed)
            } else {
                write_embedded_shell_bytes(writer, encoded.as_bytes())
            }
        }
        KeyCode::Enter => write_embedded_shell_bytes(writer, b"\r"),
        KeyCode::Backspace => write_embedded_shell_bytes(writer, b"\x7f"),
        KeyCode::Tab => write_embedded_shell_bytes(writer, b"\t"),
        KeyCode::BackTab => write_embedded_shell_bytes(writer, b"\x1b[Z"),
        KeyCode::Left if control => write_embedded_shell_bytes(writer, b"\x1b[1;5D"),
        KeyCode::Right if control => write_embedded_shell_bytes(writer, b"\x1b[1;5C"),
        KeyCode::Left => {
            let seq = if application_cursor {
                b"\x1bOD"
            } else {
                b"\x1b[D"
            };
            write_embedded_shell_bytes(writer, seq)
        }
        KeyCode::Right => {
            let seq = if application_cursor {
                b"\x1bOC"
            } else {
                b"\x1b[C"
            };
            write_embedded_shell_bytes(writer, seq)
        }
        KeyCode::Up => {
            let seq = if application_cursor {
                b"\x1bOA"
            } else {
                b"\x1b[A"
            };
            write_embedded_shell_bytes(writer, seq)
        }
        KeyCode::Down => {
            let seq = if application_cursor {
                b"\x1bOB"
            } else {
                b"\x1b[B"
            };
            write_embedded_shell_bytes(writer, seq)
        }
        KeyCode::Home => {
            let seq = if application_cursor {
                b"\x1bOH"
            } else {
                b"\x1b[H"
            };
            write_embedded_shell_bytes(writer, seq)
        }
        KeyCode::End => {
            let seq = if application_cursor {
                b"\x1bOF"
            } else {
                b"\x1b[F"
            };
            write_embedded_shell_bytes(writer, seq)
        }
        KeyCode::Delete => write_embedded_shell_bytes(writer, b"\x1b[3~"),
        KeyCode::Insert => write_embedded_shell_bytes(writer, b"\x1b[2~"),
        KeyCode::PageUp => write_embedded_shell_bytes(writer, b"\x1b[5~"),
        KeyCode::PageDown => write_embedded_shell_bytes(writer, b"\x1b[6~"),
        _ => false,
    }
}

async fn stop_embedded_shell(shell: &mut EmbeddedShellState) {
    shell.writer = None;
    shell.application_cursor = false;
    if let Some(mut child) = shell.child.take() {
        let _ = child.kill();
        let _ = child.wait();
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
