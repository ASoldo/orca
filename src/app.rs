use crate::input::Action;
use crate::model::{
    CustomResourceDef, NamespaceScope, OverviewMetrics, PodContainerInfo, ResourceTab, RowData,
    TableData,
};
use chrono::Local;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InputMode {
    Normal,
    Command,
    Filter,
    Jump,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FocusPane {
    Table,
    Detail,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DetailPaneMode {
    Dashboard,
    Details,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TableOverlayKind {
    Generic,
    PodLogs,
    RelatedLogs,
    Shell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppCommand {
    None,
    RefreshActive,
    RefreshAll,
    RefreshCustomResourceCatalog,
    LoadPodLogs {
        namespace: String,
        pod_name: String,
        container: Option<String>,
        previous: bool,
    },
    LoadResourceLogs {
        tab: ResourceTab,
        namespace: Option<String>,
        name: String,
        previous: bool,
    },
    LoadPodContainers {
        namespace: String,
        pod_name: String,
    },
    DeleteSelected {
        tab: ResourceTab,
        namespace: Option<String>,
        name: String,
    },
    RestartWorkload {
        tab: ResourceTab,
        namespace: String,
        name: String,
    },
    ScaleWorkload {
        tab: ResourceTab,
        namespace: String,
        name: String,
        replicas: i32,
    },
    ExecInPod {
        namespace: String,
        pod_name: String,
        command: Vec<String>,
    },
    OpenPodShell {
        namespace: String,
        pod_name: String,
        container: Option<String>,
        shell: String,
    },
    EditSelected {
        resource: String,
        namespace: Option<String>,
        name: String,
    },
    StartPortForward {
        tab: ResourceTab,
        namespace: String,
        name: String,
        local_port: u16,
        remote_port: u16,
    },
    SwitchContext {
        context: String,
    },
    SwitchCluster {
        cluster: String,
    },
    SwitchUser {
        user: String,
    },
}

#[derive(Debug, Clone)]
struct PendingConfirmation {
    prompt: String,
    command: AppCommand,
}

#[derive(Debug, Clone)]
struct ContainerPickerState {
    namespace: String,
    pod_name: String,
    containers: Vec<ContainerPickerEntry>,
    selected: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ContainerPickerEntry {
    pub idx: usize,
    pub pf: String,
    pub name: String,
    pub image: String,
    pub ready: String,
    pub state: String,
    pub restarts: String,
    pub age: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct FlowState {
    active_tab_index: usize,
    namespace_scope: NamespaceScope,
    filter: String,
    selected_crd: Option<String>,
    selected_indices: HashMap<ResourceTab, usize>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PortForwardSession {
    pub tab: ResourceTab,
    pub namespace: String,
    pub name: String,
    pub local_port: u16,
    pub remote_port: u16,
    pub pid: u32,
}

#[derive(Debug, Clone)]
struct ViewState {
    active_tab_index: usize,
    namespace_scope: NamespaceScope,
    filter: String,
    focus: FocusPane,
    detail_mode: DetailPaneMode,
    show_table_overview: bool,
    selected_crd: Option<String>,
    table_overlay: Option<String>,
    table_overlay_title: Option<String>,
    table_overlay_kind: TableOverlayKind,
    table_overlay_return_picker: Option<ContainerPickerState>,
    table_scroll: u16,
    detail_overlay: Option<String>,
    detail_overlay_title: Option<String>,
    detail_scroll: u16,
    container_picker: Option<ContainerPickerState>,
    flow_stack: Vec<FlowState>,
    selected_indices: HashMap<ResourceTab, usize>,
}

pub struct App {
    running: bool,
    mode: InputMode,
    focus: FocusPane,
    detail_mode: DetailPaneMode,
    tabs: Vec<ResourceTab>,
    active_tab_index: usize,
    tables: HashMap<ResourceTab, TableData>,
    namespace_scope: NamespaceScope,
    filter: String,
    input: String,
    status: String,
    show_help: bool,
    pending_g: bool,
    completion_index: usize,
    pending_confirmation: Option<PendingConfirmation>,
    cluster: String,
    context: String,
    user: String,
    table_overlay: Option<String>,
    table_overlay_title: Option<String>,
    table_overlay_kind: TableOverlayKind,
    table_overlay_return_picker: Option<ContainerPickerState>,
    show_table_overview: bool,
    table_scroll: u16,
    detail_overlay: Option<String>,
    detail_overlay_title: Option<String>,
    detail_scroll: u16,
    container_picker: Option<ContainerPickerState>,
    table_page_size: usize,
    table_view_width: u16,
    table_view_height: u16,
    detail_view_width: u16,
    detail_view_height: u16,
    discovered_crds: Vec<CustomResourceDef>,
    selected_crd: Option<String>,
    available_contexts: Vec<String>,
    available_clusters: Vec<String>,
    available_users: Vec<String>,
    active_port_forwards: Vec<PortForwardSession>,
    overview_metrics: OverviewMetrics,
    flow_stack: Vec<FlowState>,
    active_view_slot: usize,
    view_slots: Vec<Option<ViewState>>,
}

impl App {
    pub fn new(cluster: String, context: String, namespace_scope: NamespaceScope) -> Self {
        let tabs = ResourceTab::ALL.to_vec();
        let tables = tabs
            .iter()
            .copied()
            .map(|tab| (tab, TableData::default()))
            .collect::<HashMap<_, _>>();
        let initial_selected_indices = tabs
            .iter()
            .copied()
            .map(|tab| (tab, 0usize))
            .collect::<HashMap<_, _>>();
        let mut view_slots = vec![None; 10];
        let initial_slot = 1usize;
        view_slots[initial_slot] = Some(ViewState {
            active_tab_index: 0,
            namespace_scope: namespace_scope.clone(),
            filter: String::new(),
            focus: FocusPane::Table,
            detail_mode: DetailPaneMode::Dashboard,
            show_table_overview: false,
            selected_crd: None,
            table_overlay: None,
            table_overlay_title: None,
            table_overlay_kind: TableOverlayKind::Generic,
            table_overlay_return_picker: None,
            table_scroll: 0,
            detail_overlay: None,
            detail_overlay_title: None,
            detail_scroll: 0,
            container_picker: None,
            flow_stack: Vec::new(),
            selected_indices: initial_selected_indices,
        });

        Self {
            running: true,
            mode: InputMode::Normal,
            focus: FocusPane::Table,
            detail_mode: DetailPaneMode::Dashboard,
            tabs,
            active_tab_index: 0,
            tables,
            namespace_scope,
            filter: String::new(),
            input: String::new(),
            status: "Ready".to_string(),
            show_help: false,
            pending_g: false,
            completion_index: 0,
            pending_confirmation: None,
            cluster,
            context,
            user: "-".to_string(),
            table_overlay: None,
            table_overlay_title: None,
            table_overlay_kind: TableOverlayKind::Generic,
            table_overlay_return_picker: None,
            show_table_overview: false,
            table_scroll: 0,
            detail_overlay: None,
            detail_overlay_title: None,
            detail_scroll: 0,
            container_picker: None,
            table_page_size: 10,
            table_view_width: 80,
            table_view_height: 20,
            detail_view_width: 80,
            detail_view_height: 20,
            discovered_crds: Vec::new(),
            selected_crd: None,
            available_contexts: Vec::new(),
            available_clusters: Vec::new(),
            available_users: Vec::new(),
            active_port_forwards: Vec::new(),
            overview_metrics: OverviewMetrics::default(),
            flow_stack: Vec::new(),
            active_view_slot: initial_slot,
            view_slots,
        }
    }

    pub fn running(&self) -> bool {
        self.running
    }

    pub fn mode(&self) -> InputMode {
        self.mode
    }

    pub fn detail_mode(&self) -> DetailPaneMode {
        self.detail_mode
    }

    pub fn tabs(&self) -> &[ResourceTab] {
        &self.tabs
    }

    pub fn active_tab(&self) -> ResourceTab {
        self.tabs[self.active_tab_index]
    }

    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    pub fn context(&self) -> &str {
        &self.context
    }

    pub fn user(&self) -> &str {
        &self.user
    }

    pub fn set_kube_target(
        &mut self,
        cluster: String,
        context: String,
        user: String,
        default_namespace: String,
        preserve_all_namespaces: bool,
    ) {
        self.cluster = cluster;
        self.context = context;
        self.user = user;
        if preserve_all_namespaces && matches!(self.namespace_scope, NamespaceScope::All) {
            return;
        }
        self.namespace_scope = NamespaceScope::Named(default_namespace);
    }

    pub fn set_kube_catalog(
        &mut self,
        contexts: Vec<String>,
        clusters: Vec<String>,
        users: Vec<String>,
    ) {
        self.available_contexts = contexts;
        self.available_contexts.sort();
        self.available_contexts.dedup();

        self.available_clusters = clusters;
        self.available_clusters.sort();
        self.available_clusters.dedup();

        self.available_users = users;
        self.available_users.sort();
        self.available_users.dedup();
    }

    pub fn set_user(&mut self, user: String) {
        self.user = user;
    }

    pub fn namespace_scope(&self) -> &NamespaceScope {
        &self.namespace_scope
    }

    pub fn filter(&self) -> &str {
        &self.filter
    }

    pub fn input(&self) -> &str {
        &self.input
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn show_help(&self) -> bool {
        self.show_help
    }

    pub fn pending_confirmation_prompt(&self) -> Option<&str> {
        self.pending_confirmation
            .as_ref()
            .map(|pending| pending.prompt.as_str())
    }

    pub fn detail_scroll(&self) -> u16 {
        self.detail_scroll
    }

    pub fn table_scroll(&self) -> u16 {
        self.table_scroll
    }

    pub fn table_overlay_active(&self) -> bool {
        self.table_overlay.is_some()
    }

    pub fn shell_overlay_active(&self) -> bool {
        self.table_overlay_active() && self.table_overlay_kind == TableOverlayKind::Shell
    }

    pub fn table_overlay_kind(&self) -> Option<TableOverlayKind> {
        self.table_overlay.as_ref().map(|_| self.table_overlay_kind)
    }

    pub fn container_picker_active(&self) -> bool {
        self.container_picker.is_some()
    }

    pub fn pane_label(&self) -> &'static str {
        if self.container_picker_active() {
            return "ctr";
        }
        if self.table_overlay_active() {
            return match self.table_overlay_kind {
                TableOverlayKind::PodLogs => "log",
                TableOverlayKind::RelatedLogs => "LOG",
                TableOverlayKind::Shell => "sh",
                TableOverlayKind::Generic => "out",
            };
        }
        if self.show_table_overview {
            return "ovr";
        }
        if self.detail_mode == DetailPaneMode::Details {
            return "det";
        }
        match self.focus {
            FocusPane::Table => "tbl",
            FocusPane::Detail => "det",
        }
    }

    pub fn table_overview_active(&self) -> bool {
        self.show_table_overview
    }

    pub fn table_overlay_title(&self) -> Option<&str> {
        self.table_overlay_title.as_deref()
    }

    pub fn table_overlay_text(&self) -> Option<&str> {
        self.table_overlay.as_deref()
    }

    pub fn container_picker_title(&self) -> Option<String> {
        self.container_picker
            .as_ref()
            .map(|picker| format!("Containers {}/{}", picker.namespace, picker.pod_name))
    }

    pub fn container_picker_headers(&self) -> Vec<String> {
        vec![
            "Idx".to_string(),
            "Name".to_string(),
            "Container".to_string(),
            "Image".to_string(),
            "Ready".to_string(),
            "State".to_string(),
            "Restart".to_string(),
            "Age".to_string(),
            "PF".to_string(),
        ]
    }

    pub fn container_picker_pod_name(&self) -> Option<String> {
        self.container_picker
            .as_ref()
            .map(|picker| picker.pod_name.clone())
    }

    pub fn container_picker_items(&self) -> Vec<ContainerPickerEntry> {
        self.container_picker
            .as_ref()
            .map(|picker| picker.containers.clone())
            .unwrap_or_default()
    }

    pub fn container_picker_selected_index(&self) -> Option<usize> {
        self.container_picker.as_ref().map(|picker| picker.selected)
    }

    pub fn set_container_picker(
        &mut self,
        namespace: impl Into<String>,
        pod_name: impl Into<String>,
        containers: Vec<PodContainerInfo>,
    ) {
        let namespace = namespace.into();
        let pod_name = pod_name.into();
        let pf = self.port_forward_cell_for_target(ResourceTab::Pods, &namespace, &pod_name);

        let mut entries = containers
            .into_iter()
            .filter(|container| !container.name.trim().is_empty())
            .enumerate()
            .map(|(index, container)| ContainerPickerEntry {
                idx: index.saturating_add(1),
                pf: pf.clone(),
                name: container.name,
                image: if container.image.trim().is_empty() {
                    "-".to_string()
                } else {
                    container.image
                },
                ready: if container.ready {
                    "true".to_string()
                } else {
                    "false".to_string()
                },
                state: if container.state.trim().is_empty() {
                    "-".to_string()
                } else {
                    container.state
                },
                restarts: container.restarts.to_string(),
                age: if container.age.trim().is_empty() {
                    "-".to_string()
                } else {
                    container.age
                },
            })
            .collect::<Vec<_>>();

        entries.sort_by(|left, right| left.idx.cmp(&right.idx));
        if entries.is_empty() {
            self.container_picker = None;
            self.status = "No containers found for selected pod".to_string();
            return;
        }

        self.container_picker = Some(ContainerPickerState {
            namespace,
            pod_name,
            containers: entries,
            selected: 0,
        });
        self.show_table_overview = false;
        self.clear_table_overlay();
        self.clear_detail_overlay();
        self.detail_mode = DetailPaneMode::Dashboard;
        self.detail_scroll = 0;
        self.focus = FocusPane::Table;
    }

    pub fn overview_metrics(&self) -> &OverviewMetrics {
        &self.overview_metrics
    }

    pub fn selected_resource_usage(&self) -> Option<(u64, u64)> {
        let row = self.active_selected_row()?;
        if matches!(self.active_tab(), ResourceTab::Pods) {
            let namespace = row.namespace.as_deref()?;
            let key = format!("{namespace}/{}", row.name);
            if let Some(usage) = self.overview_metrics.pod_usage.get(&key) {
                return Some(*usage);
            }
        }

        if let Some(namespace) = row.namespace.as_deref()
            && let Some(usage) = self.overview_metrics.namespace_usage.get(namespace)
        {
            return Some(*usage);
        }

        if self.overview_metrics.cpu_usage_millicores > 0
            || self.overview_metrics.memory_usage_bytes > 0
        {
            return Some((
                self.overview_metrics.cpu_usage_millicores,
                self.overview_metrics.memory_usage_bytes,
            ));
        }

        None
    }

    pub fn has_completion_mode(&self) -> bool {
        matches!(
            self.mode,
            InputMode::Command | InputMode::Filter | InputMode::Jump
        )
    }

    pub fn completion_candidates(&self) -> Vec<String> {
        match self.mode {
            InputMode::Normal | InputMode::Filter => Vec::new(),
            InputMode::Command => self.command_completions(),
            InputMode::Jump => self.jump_completions(),
        }
    }

    pub fn completion_index(&self) -> usize {
        self.completion_index
    }

    pub fn active_view_slot(&self) -> usize {
        self.active_view_slot
    }

    pub fn view_slot_initialized(&self, slot: usize) -> bool {
        slot == self.active_view_slot
            || self
                .view_slots
                .get(slot)
                .and_then(|entry| entry.as_ref())
                .is_some()
    }

    pub fn visible_view_slots(&self) -> Vec<usize> {
        let mut slots = self
            .view_slots
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| entry.as_ref().map(|_| index))
            .collect::<Vec<_>>();
        if !slots.contains(&self.active_view_slot) {
            slots.push(self.active_view_slot);
        }
        slots.sort_unstable();
        slots
    }

    pub fn active_last_refresh(&self) -> Option<String> {
        self.tables
            .get(&self.active_tab())
            .and_then(|table| table.last_refreshed)
            .map(|ts| ts.format("%Y-%m-%d %H:%M:%S").to_string())
    }

    pub fn active_headers(&self) -> Vec<String> {
        self.tables
            .get(&self.active_tab())
            .map(|table| table.headers.clone())
            .unwrap_or_default()
    }

    pub fn active_visible_rows(&self) -> Vec<&RowData> {
        self.visible_rows_for(self.active_tab())
    }

    pub fn active_visible_error(&self) -> Option<&str> {
        self.tables
            .get(&self.active_tab())
            .and_then(|table| table.error.as_deref())
    }

    pub fn active_selected_index(&self) -> Option<usize> {
        let visible_len = self.active_visible_len();
        if visible_len == 0 {
            return None;
        }

        let table = self.tables.get(&self.active_tab())?;
        Some(table.selected.min(visible_len.saturating_sub(1)))
    }

    pub fn active_selected_row(&self) -> Option<&RowData> {
        let selected = self.active_selected_index()?;
        self.active_visible_rows().get(selected).copied()
    }

    pub fn register_port_forward(
        &mut self,
        tab: ResourceTab,
        namespace: String,
        name: String,
        local_port: u16,
        remote_port: u16,
        pid: u32,
    ) {
        self.active_port_forwards.retain(|existing| {
            !(existing.tab == tab
                && existing.namespace == namespace
                && existing.name == name
                && existing.local_port == local_port
                && existing.remote_port == remote_port)
        });

        self.active_port_forwards.push(PortForwardSession {
            tab,
            namespace,
            name,
            local_port,
            remote_port,
            pid,
        });
    }

    pub fn remove_port_forward_by_pid(&mut self, pid: u32) -> Option<PortForwardSession> {
        let index = self
            .active_port_forwards
            .iter()
            .position(|session| session.pid == pid)?;
        Some(self.active_port_forwards.remove(index))
    }

    pub fn port_forward_badge(&self) -> Option<String> {
        let row = self.active_selected_row()?;
        let namespace = row.namespace.as_deref()?;
        self.port_forward_badge_for_target(self.active_tab(), namespace, &row.name)
    }

    fn port_forward_badge_for_target(
        &self,
        tab: ResourceTab,
        namespace: &str,
        name: &str,
    ) -> Option<String> {
        let matches = self
            .active_port_forwards
            .iter()
            .filter(|session| {
                session.tab == tab && session.namespace == namespace && session.name == name
            })
            .collect::<Vec<_>>();

        let first = matches.first()?;
        if matches.len() == 1 {
            Some(format!("󰕒 {}→{}", first.local_port, first.remote_port))
        } else {
            Some(format!(
                "󰕒 {}→{} +{}",
                first.local_port,
                first.remote_port,
                matches.len().saturating_sub(1)
            ))
        }
    }

    pub fn port_forward_cell_for_row(&self, tab: ResourceTab, row: &RowData) -> String {
        let Some(namespace) = row.namespace.as_deref() else {
            return "-".to_string();
        };
        self.port_forward_cell_for_target(tab, namespace, &row.name)
    }

    fn port_forward_cell_for_target(
        &self,
        tab: ResourceTab,
        namespace: &str,
        name: &str,
    ) -> String {
        let sessions = self
            .active_port_forwards
            .iter()
            .filter(|session| {
                session.tab == tab && session.namespace == namespace && session.name == name
            })
            .collect::<Vec<_>>();

        let Some(first) = sessions.first() else {
            return "-".to_string();
        };

        if sessions.len() == 1 {
            format!("{}→{}", first.local_port, first.remote_port)
        } else {
            format!(
                "{}→{} (+{})",
                first.local_port,
                first.remote_port,
                sessions.len().saturating_sub(1)
            )
        }
    }

    pub fn detail_title(&self) -> String {
        if let Some(title) = &self.detail_overlay_title {
            title.clone()
        } else {
            format!("{} Details", self.active_tab().title())
        }
    }

    pub fn detail_text(&self) -> String {
        if let Some(overlay) = &self.detail_overlay {
            return overlay.clone();
        }

        self.active_selected_row()
            .map(|row| row.detail.clone())
            .unwrap_or_else(|| "No resource selected".to_string())
    }

    pub fn detail_overlay_active(&self) -> bool {
        self.detail_overlay.is_some()
    }

    pub fn set_pod_logs_overlay(&mut self, title: impl Into<String>, detail: String) {
        self.set_table_overlay_with_kind(title, detail, TableOverlayKind::PodLogs);
    }

    pub fn set_related_logs_overlay(&mut self, title: impl Into<String>, detail: String) {
        self.set_table_overlay_with_kind(title, detail, TableOverlayKind::RelatedLogs);
    }

    pub fn set_shell_overlay(&mut self, title: impl Into<String>, detail: String) {
        self.set_table_overlay_with_kind(title, detail, TableOverlayKind::Shell);
        self.table_scroll = self.table_max_scroll();
    }

    pub fn append_shell_output(&mut self, chunk: &str) {
        if !self.shell_overlay_active() {
            return;
        }

        let Some(overlay) = self.table_overlay.as_mut() else {
            return;
        };

        overlay.push_str(chunk);
        const MAX_OVERLAY_CHARS: usize = 500_000;
        if overlay.chars().count() > MAX_OVERLAY_CHARS {
            let trimmed = overlay
                .chars()
                .rev()
                .take(MAX_OVERLAY_CHARS)
                .collect::<String>()
                .chars()
                .rev()
                .collect::<String>();
            *overlay = trimmed;
        }
        self.table_scroll = self.table_max_scroll();
    }

    pub fn replace_shell_output(&mut self, snapshot: String) {
        if !self.shell_overlay_active() {
            return;
        }

        let Some(overlay) = self.table_overlay.as_mut() else {
            return;
        };

        *overlay = snapshot;
        self.table_scroll = self.table_max_scroll();
    }

    fn set_table_overlay_with_kind(
        &mut self,
        title: impl Into<String>,
        detail: String,
        kind: TableOverlayKind,
    ) {
        self.table_overlay_title = Some(title.into());
        self.table_overlay = Some(detail);
        self.table_overlay_kind = kind;
        self.table_overlay_return_picker = self.container_picker.clone();
        self.container_picker = None;
        self.show_table_overview = false;
        self.table_scroll = 0;
        self.focus = FocusPane::Table;
        self.detail_mode = DetailPaneMode::Dashboard;
        self.clear_detail_overlay();
    }

    pub fn set_overview_metrics(&mut self, metrics: OverviewMetrics) {
        self.overview_metrics = metrics;
    }

    pub fn set_table_page_size(&mut self, rows: usize) {
        self.table_page_size = rows.max(1);
    }

    pub fn set_table_viewport(&mut self, width: u16, height: u16) {
        self.table_view_width = width.max(1);
        self.table_view_height = height.max(1);
        self.table_scroll = self.table_scroll.min(self.table_max_scroll());
    }

    pub fn set_detail_viewport(&mut self, width: u16, height: u16) {
        self.detail_view_width = width.max(1);
        self.detail_view_height = height.max(1);
        self.detail_scroll = self.detail_scroll.min(self.detail_max_scroll());
    }

    pub fn apply_action(&mut self, action: Action) -> AppCommand {
        if let Some(pending) = self.pending_confirmation.take() {
            match action {
                Action::ConfirmYes | Action::EnterResource => {
                    self.status = format!("Confirmed: {}", pending.prompt);
                    return pending.command;
                }
                Action::ConfirmNo | Action::CancelInput | Action::ClearDetailOverlay => {
                    self.status = "Action cancelled".to_string();
                    return AppCommand::None;
                }
                _ => {
                    self.pending_confirmation = Some(pending);
                    self.status =
                        "Pending confirmation: press y to confirm or n to cancel".to_string();
                    return AppCommand::None;
                }
            }
        }

        if !matches!(action, Action::GPrefix) {
            self.pending_g = false;
        }

        if self.show_help && !matches!(action, Action::ToggleHelp) {
            self.show_help = false;
        }

        match action {
            Action::Quit => {
                self.running = false;
                self.status = "Exit requested".to_string();
                AppCommand::None
            }
            Action::NextTab => self.switch_tab_by_offset(1),
            Action::PrevTab => self.switch_tab_by_offset(-1),
            Action::Down => {
                if self.container_picker_active() {
                    self.move_container_selection(1);
                } else if self.focus == FocusPane::Detail {
                    self.scroll_detail(1);
                } else if self.table_overlay_active() {
                    self.scroll_table_overlay(1);
                } else {
                    self.move_selection(1);
                }
                AppCommand::None
            }
            Action::Up => {
                if self.container_picker_active() {
                    self.move_container_selection(-1);
                } else if self.focus == FocusPane::Detail {
                    self.scroll_detail(-1);
                } else if self.table_overlay_active() {
                    self.scroll_table_overlay(-1);
                } else {
                    self.move_selection(-1);
                }
                AppCommand::None
            }
            Action::PageDown => {
                if self.container_picker_active() {
                    self.move_container_selection(self.table_page_step());
                } else if self.focus == FocusPane::Detail {
                    self.scroll_detail(self.detail_page_step() as isize);
                } else if self.table_overlay_active() {
                    self.scroll_table_overlay(self.table_page_step());
                } else {
                    self.move_selection(self.table_page_step());
                }
                AppCommand::None
            }
            Action::PageUp => {
                if self.container_picker_active() {
                    self.move_container_selection(-self.table_page_step());
                } else if self.focus == FocusPane::Detail {
                    self.scroll_detail(-(self.detail_page_step() as isize));
                } else if self.table_overlay_active() {
                    self.scroll_table_overlay(-self.table_page_step());
                } else {
                    self.move_selection(-self.table_page_step());
                }
                AppCommand::None
            }
            Action::Top => {
                if self.container_picker_active() {
                    self.select_container_first();
                } else if self.focus == FocusPane::Detail {
                    self.detail_scroll = 0;
                } else if self.table_overlay_active() {
                    self.table_scroll = 0;
                } else {
                    self.select_first();
                }
                AppCommand::None
            }
            Action::Bottom => {
                if self.container_picker_active() {
                    self.select_container_last();
                } else if self.focus == FocusPane::Detail {
                    self.detail_scroll = self.detail_max_scroll();
                } else if self.table_overlay_active() {
                    self.table_scroll = self.table_max_scroll();
                } else {
                    self.select_last();
                }
                AppCommand::None
            }
            Action::ToggleHelp => {
                self.show_help = !self.show_help;
                AppCommand::None
            }
            Action::ToggleFocus => {
                if self.detail_mode != DetailPaneMode::Details {
                    self.focus = FocusPane::Table;
                    self.status = "Open details with d".to_string();
                    return AppCommand::None;
                }
                self.focus = match self.focus {
                    FocusPane::Table => FocusPane::Detail,
                    FocusPane::Detail => FocusPane::Table,
                };
                AppCommand::None
            }
            Action::EnterResource => self.enter_selected_resource(),
            Action::ShowDetails => self.open_selected_details(),
            Action::StartCommand => {
                self.mode = InputMode::Command;
                self.input.clear();
                self.completion_index = 0;
                self.status = "Command mode (:help for commands)".to_string();
                AppCommand::None
            }
            Action::StartJump => {
                self.mode = InputMode::Jump;
                self.input.clear();
                self.completion_index = 0;
                self.status = "Jump mode (> <tab> <query>)".to_string();
                AppCommand::None
            }
            Action::StartFilter => {
                self.mode = InputMode::Filter;
                self.input = self.filter.clone();
                self.completion_index = 0;
                self.status = "Filter mode".to_string();
                AppCommand::None
            }
            Action::Refresh => {
                self.status = format!(
                    "Refreshing {} in namespace '{}'",
                    self.active_tab().title(),
                    self.namespace_scope
                );
                AppCommand::RefreshActive
            }
            Action::LoadPodLogs => self.create_logs_command(false),
            Action::LoadResourceLogs => self.create_related_logs_command(true),
            Action::OpenPodShell => self.prepare_shell_command(None, "auto".to_string()),
            Action::EditResource => self.prepare_edit_command(),
            Action::StartPortForwardPrompt => {
                self.mode = InputMode::Command;
                self.input = "port-forward ".to_string();
                self.completion_index = 0;
                self.status = "Port-forward mode (:port-forward <local>:<remote>)".to_string();
                AppCommand::None
            }
            Action::ToggleOverview => {
                self.show_table_overview = !self.show_table_overview;
                if self.show_table_overview {
                    self.clear_table_overlay();
                    self.clear_container_picker();
                    self.clear_detail_overlay();
                    self.detail_mode = DetailPaneMode::Dashboard;
                    self.detail_scroll = 0;
                    self.focus = FocusPane::Table;
                    self.status = format!("Opened {} overview", self.active_tab().title());
                } else {
                    self.status = "Closed overview".to_string();
                }
                AppCommand::None
            }
            Action::ClearDetailOverlay => {
                if self.container_picker_active() {
                    self.container_picker = None;
                    if self.pop_flow_state() {
                        self.status = "Back to previous flow step".to_string();
                    } else {
                        self.status = "Closed container list".to_string();
                    }
                } else if self.table_overlay_active() {
                    if let Some(previous_picker) = self.table_overlay_return_picker.clone() {
                        self.clear_table_overlay();
                        self.container_picker = Some(previous_picker);
                        self.status = "Back to container list".to_string();
                    } else {
                        let was_shell = self.shell_overlay_active();
                        self.clear_table_overlay();
                        self.status = if was_shell {
                            "Closed shell view".to_string()
                        } else {
                            "Closed logs view".to_string()
                        };
                    }
                } else if self.show_table_overview {
                    self.show_table_overview = false;
                    self.status = "Closed overview".to_string();
                } else if self.detail_mode == DetailPaneMode::Details
                    || self.focus == FocusPane::Detail
                {
                    self.dismiss_detail_view();
                    self.status = "Closed details".to_string();
                } else if self.pop_flow_state() {
                    self.status = "Back to previous flow step".to_string();
                } else {
                    self.status = "At flow root".to_string();
                }
                AppCommand::None
            }
            Action::GPrefix => {
                if self.pending_g {
                    self.pending_g = false;
                    if self.focus == FocusPane::Detail {
                        self.detail_scroll = 0;
                    } else if self.table_overlay_active() {
                        self.table_scroll = 0;
                    } else {
                        self.select_first();
                    }
                } else {
                    self.pending_g = true;
                }
                AppCommand::None
            }
            Action::SubmitInput => self.submit_input(),
            Action::CompleteInput => {
                self.apply_completion();
                AppCommand::None
            }
            Action::NextSuggestion => {
                self.bump_completion(1);
                AppCommand::None
            }
            Action::PrevSuggestion => {
                self.bump_completion(-1);
                AppCommand::None
            }
            Action::CancelInput => {
                self.mode = InputMode::Normal;
                self.input.clear();
                self.completion_index = 0;
                self.status = "Input cancelled".to_string();
                AppCommand::None
            }
            Action::Backspace => {
                self.input.pop();
                self.completion_index = 0;
                AppCommand::None
            }
            Action::Delete => {
                while self.input.ends_with(' ') {
                    self.input.pop();
                }
                while !self.input.ends_with(' ') && !self.input.is_empty() {
                    self.input.pop();
                }
                self.completion_index = 0;
                AppCommand::None
            }
            Action::InputChar(c) => {
                self.input.push(c);
                self.completion_index = 0;
                AppCommand::None
            }
            Action::ConfirmYes | Action::ConfirmNo => {
                self.status = "No pending confirmation".to_string();
                AppCommand::None
            }
            Action::SwitchView(slot) => self.switch_view_slot(slot as usize),
            Action::DeleteView(slot) => self.delete_view_slot(slot as usize),
        }
    }

    pub fn set_active_table_data(&mut self, tab: ResourceTab, mut table: TableData) {
        let selected_identity = self.selected_row_identity_for_tab(tab);
        let previous_selected = self.selected_index_for_tab(tab);
        table.selected = table.selected.min(table.rows.len().saturating_sub(1));
        self.tables.insert(tab, table);
        if let Some((namespace, name)) = selected_identity {
            self.select_row_by_identity_with_fallback(tab, namespace, &name, previous_selected);
        } else {
            self.set_selected_index_for_tab(tab, previous_selected);
        }
        self.status = format!("{} updated", tab.title());
    }

    pub fn set_active_tab_error(&mut self, tab: ResourceTab, error: impl Into<String>) {
        let now = Local::now();
        let error = error.into();

        if let Some(table) = self.tables.get_mut(&tab) {
            table.set_error(error.clone(), now);
        }

        let summary = summarize_error_line(&error);
        self.status = normalize_status_text(format!("{} refresh failed: {summary}", tab.title()));
    }

    pub fn set_detail_overlay(&mut self, title: impl Into<String>, detail: String) {
        self.detail_overlay_title = Some(title.into());
        self.detail_overlay = Some(detail);
        self.detail_mode = DetailPaneMode::Details;
        self.focus = FocusPane::Detail;
        self.detail_scroll = 0;
    }

    pub fn set_status(&mut self, status: impl Into<String>) {
        self.status = normalize_status_text(status.into());
    }

    pub fn set_custom_resources(&mut self, mut crds: Vec<CustomResourceDef>) {
        crds.sort_by(|left, right| left.name.cmp(&right.name));
        self.discovered_crds = crds;

        if self.discovered_crds.is_empty() {
            self.selected_crd = None;
            self.status = "No CRDs discovered".to_string();
            return;
        }

        let existing = self
            .selected_crd
            .clone()
            .filter(|selected| self.discovered_crds.iter().any(|crd| &crd.name == selected));

        self.selected_crd =
            existing.or_else(|| self.discovered_crds.first().map(|crd| crd.name.clone()));
        self.status = format!(
            "Discovered {} CRDs (active: {})",
            self.discovered_crds.len(),
            self.selected_crd.as_deref().unwrap_or("-")
        );
    }

    pub fn selected_custom_resource(&self) -> Option<&CustomResourceDef> {
        let selected = self.selected_crd.as_deref()?;
        self.discovered_crds.iter().find(|crd| crd.name == selected)
    }

    fn visible_rows_for(&self, tab: ResourceTab) -> Vec<&RowData> {
        let Some(table) = self.tables.get(&tab) else {
            return Vec::new();
        };

        table
            .rows
            .iter()
            .filter(|row| row.matches_filter(&self.filter))
            .collect()
    }

    fn active_visible_len(&self) -> usize {
        self.visible_rows_for(self.active_tab()).len()
    }

    fn move_selection(&mut self, delta: isize) {
        let visible_len = self.active_visible_len();
        let table = self.tables.get_mut(&self.active_tab());

        let Some(table) = table else {
            return;
        };

        if visible_len == 0 {
            table.selected = 0;
            return;
        }

        let max_index = visible_len.saturating_sub(1) as isize;
        let current = table.selected.min(max_index as usize) as isize;
        let next = (current + delta).clamp(0, max_index) as usize;
        table.selected = next;
    }

    fn move_container_selection(&mut self, delta: isize) {
        let Some(picker) = self.container_picker.as_mut() else {
            return;
        };
        if picker.containers.is_empty() {
            picker.selected = 0;
            return;
        }
        let max_index = picker.containers.len().saturating_sub(1) as isize;
        let current = picker.selected.min(max_index as usize) as isize;
        let next = (current + delta).clamp(0, max_index) as usize;
        picker.selected = next;
    }

    fn select_container_first(&mut self) {
        if let Some(picker) = self.container_picker.as_mut() {
            picker.selected = 0;
        }
    }

    fn select_container_last(&mut self) {
        if let Some(picker) = self.container_picker.as_mut() {
            picker.selected = picker.containers.len().saturating_sub(1);
        }
    }

    fn select_first(&mut self) {
        if let Some(table) = self.tables.get_mut(&self.active_tab()) {
            table.selected = 0;
        }
    }

    fn select_last(&mut self) {
        let visible_len = self.active_visible_len();
        if let Some(table) = self.tables.get_mut(&self.active_tab()) {
            table.selected = visible_len.saturating_sub(1);
        }
    }

    fn clamp_active_selection(&mut self) {
        self.clamp_selection_for_tab(self.active_tab());
    }

    fn clamp_selection_for_tab(&mut self, tab: ResourceTab) {
        let filter = self.filter.clone();
        if let Some(table) = self.tables.get_mut(&tab) {
            let visible_len = table
                .rows
                .iter()
                .filter(|row| row.matches_filter(&filter))
                .count();
            table.selected = table.selected.min(visible_len.saturating_sub(1));
        }
    }

    fn clamp_all_selections(&mut self) {
        let filter = self.filter.clone();
        for table in self.tables.values_mut() {
            let visible_len = table
                .rows
                .iter()
                .filter(|row| row.matches_filter(&filter))
                .count();
            table.selected = table.selected.min(visible_len.saturating_sub(1));
        }
    }

    fn capture_flow_state(&self) -> FlowState {
        let selected_indices = self
            .tables
            .iter()
            .map(|(tab, table)| (*tab, table.selected))
            .collect::<HashMap<_, _>>();
        FlowState {
            active_tab_index: self.active_tab_index,
            namespace_scope: self.namespace_scope.clone(),
            filter: self.filter.clone(),
            selected_crd: self.selected_crd.clone(),
            selected_indices,
        }
    }

    fn apply_flow_state(&mut self, state: &FlowState) {
        self.active_tab_index = state
            .active_tab_index
            .min(self.tabs.len().saturating_sub(1));
        self.namespace_scope = state.namespace_scope.clone();
        self.filter = state.filter.clone();
        self.selected_crd = state.selected_crd.clone();

        let tabs = self.tabs.clone();
        for tab in tabs {
            let selected = state.selected_indices.get(&tab).copied().unwrap_or(0);
            self.set_selected_index_for_tab(tab, selected);
        }
        self.clamp_all_selections();

        self.clear_table_overlay();
        self.clear_container_picker();
        self.clear_detail_overlay();
        self.show_table_overview = false;
        self.focus = FocusPane::Table;
        self.detail_mode = DetailPaneMode::Dashboard;
        self.table_scroll = 0;
        self.detail_scroll = 0;
    }

    fn reset_flow_root(&mut self) {
        self.flow_stack.clear();
    }

    fn push_flow_state(&mut self) {
        let snapshot = self.capture_flow_state();
        let should_push = self
            .flow_stack
            .last()
            .map(|state| state != &snapshot)
            .unwrap_or(true);
        if should_push {
            self.flow_stack.push(snapshot);
        }
    }

    fn pop_flow_state(&mut self) -> bool {
        let Some(state) = self.flow_stack.pop() else {
            return false;
        };
        self.apply_flow_state(&state);
        true
    }

    fn switch_tab_by_offset(&mut self, delta: isize) -> AppCommand {
        if self.tabs.is_empty() {
            return AppCommand::None;
        }

        let len = self.tabs.len() as isize;
        let current = self.active_tab_index as isize;
        let next = (current + delta).rem_euclid(len) as usize;
        self.active_tab_index = next;
        self.on_tab_changed()
    }

    fn capture_view_state(&self) -> ViewState {
        let selected_indices = self
            .tables
            .iter()
            .map(|(tab, table)| (*tab, table.selected))
            .collect::<HashMap<_, _>>();
        ViewState {
            active_tab_index: self.active_tab_index,
            namespace_scope: self.namespace_scope.clone(),
            filter: self.filter.clone(),
            focus: self.focus,
            detail_mode: self.detail_mode,
            show_table_overview: self.show_table_overview,
            selected_crd: self.selected_crd.clone(),
            table_overlay: self.table_overlay.clone(),
            table_overlay_title: self.table_overlay_title.clone(),
            table_overlay_kind: self.table_overlay_kind,
            table_overlay_return_picker: self.table_overlay_return_picker.clone(),
            table_scroll: self.table_scroll,
            detail_overlay: self.detail_overlay.clone(),
            detail_overlay_title: self.detail_overlay_title.clone(),
            detail_scroll: self.detail_scroll,
            container_picker: self.container_picker.clone(),
            flow_stack: self.flow_stack.clone(),
            selected_indices,
        }
    }

    fn apply_view_state(&mut self, state: &ViewState) {
        self.active_tab_index = state
            .active_tab_index
            .min(self.tabs.len().saturating_sub(1));
        self.namespace_scope = state.namespace_scope.clone();
        self.filter = state.filter.clone();
        self.focus = state.focus;
        self.detail_mode = state.detail_mode;
        self.show_table_overview = state.show_table_overview;
        self.selected_crd = state.selected_crd.clone();
        self.table_overlay = state.table_overlay.clone();
        self.table_overlay_title = state.table_overlay_title.clone();
        self.table_overlay_kind = state.table_overlay_kind;
        self.table_overlay_return_picker = state.table_overlay_return_picker.clone();
        self.table_scroll = state.table_scroll;
        self.detail_overlay = state.detail_overlay.clone();
        self.detail_overlay_title = state.detail_overlay_title.clone();
        self.detail_scroll = state.detail_scroll;
        self.container_picker = state.container_picker.clone();
        self.flow_stack = state.flow_stack.clone();

        let tabs = self.tabs.clone();
        for tab in tabs {
            let selected = state.selected_indices.get(&tab).copied().unwrap_or(0);
            self.set_selected_index_for_tab(tab, selected);
        }
        self.clamp_all_selections();
        self.table_scroll = self.table_scroll.min(self.table_max_scroll());
        self.detail_scroll = self.detail_scroll.min(self.detail_max_scroll());
    }

    fn switch_view_slot(&mut self, slot: usize) -> AppCommand {
        if slot >= self.view_slots.len() {
            self.status = format!("Invalid view slot {slot}");
            return AppCommand::None;
        }
        if slot == self.active_view_slot {
            self.status = format!("View {slot} already active");
            return AppCommand::None;
        }

        let current_slot = self.active_view_slot;
        self.view_slots[current_slot] = Some(self.capture_view_state());

        let target_state = if let Some(state) = self.view_slots[slot].clone() {
            state
        } else {
            let mut state = self.capture_view_state();
            state.filter.clear();
            state.focus = FocusPane::Table;
            state.detail_mode = DetailPaneMode::Dashboard;
            state.show_table_overview = false;
            state.table_overlay = None;
            state.table_overlay_title = None;
            state.table_overlay_kind = TableOverlayKind::Generic;
            state.table_overlay_return_picker = None;
            state.table_scroll = 0;
            state.detail_overlay = None;
            state.detail_overlay_title = None;
            state.detail_scroll = 0;
            state.container_picker = None;
            state.flow_stack = Vec::new();
            state
        };

        self.active_view_slot = slot;
        self.apply_view_state(&target_state);
        self.view_slots[slot] = Some(self.capture_view_state());
        self.mode = InputMode::Normal;
        self.input.clear();
        self.completion_index = 0;
        self.pending_g = false;
        self.status = format!("Switched to view {slot} (refreshing)");
        AppCommand::RefreshActive
    }

    fn delete_view_slot(&mut self, slot: usize) -> AppCommand {
        if slot >= self.view_slots.len() {
            self.status = format!("Invalid view slot {slot}");
            return AppCommand::None;
        }

        if slot != self.active_view_slot {
            if self.view_slots[slot].is_none() {
                self.status = format!("View {slot} is already empty");
                return AppCommand::None;
            }
            self.view_slots[slot] = None;
            self.status = format!("Deleted view {slot}");
            return AppCommand::None;
        }

        let mut fallback_slots = self
            .view_slots
            .iter()
            .enumerate()
            .filter_map(|(index, state)| (index != slot && state.is_some()).then_some(index))
            .collect::<Vec<_>>();
        fallback_slots.sort_unstable();

        let Some(fallback) = fallback_slots
            .iter()
            .copied()
            .find(|candidate| *candidate == 1)
            .or_else(|| fallback_slots.first().copied())
        else {
            self.status =
                format!("Cannot delete active view {slot}: at least one view must remain");
            return AppCommand::None;
        };

        let Some(target_state) = self.view_slots[fallback].clone() else {
            self.status = format!("View {fallback} has no state to switch to");
            return AppCommand::None;
        };

        self.view_slots[slot] = None;
        self.active_view_slot = fallback;
        self.apply_view_state(&target_state);
        self.view_slots[fallback] = Some(self.capture_view_state());
        self.mode = InputMode::Normal;
        self.input.clear();
        self.completion_index = 0;
        self.pending_g = false;
        self.status = format!("Deleted view {slot}; switched to {fallback}");
        AppCommand::RefreshActive
    }

    fn switch_to_tab(&mut self, target: ResourceTab) -> AppCommand {
        if let Some(index) = self.tabs.iter().position(|tab| *tab == target) {
            self.active_tab_index = index;
            return self.on_tab_changed();
        }

        self.status = format!("Tab '{}' is not available", target.title());
        AppCommand::None
    }

    fn on_tab_changed(&mut self) -> AppCommand {
        self.dismiss_detail_view();
        self.clear_table_overlay();
        self.clear_container_picker();
        self.detail_scroll = 0;
        self.status = format!("Switched to {}", self.active_tab().title());
        if self
            .tables
            .get(&self.active_tab())
            .is_some_and(|table| table.rows.is_empty() && table.error.is_none())
        {
            AppCommand::RefreshActive
        } else {
            self.clamp_active_selection();
            AppCommand::None
        }
    }

    fn bump_completion(&mut self, direction: isize) {
        let completion_len = self.completion_candidates().len();
        if completion_len == 0 {
            self.completion_index = 0;
            return;
        }

        self.completion_index = (self.completion_index as isize + direction)
            .rem_euclid(completion_len as isize) as usize;
    }

    fn apply_completion(&mut self) {
        let completions = self.completion_candidates();
        if completions.is_empty() {
            return;
        }

        let index = self
            .completion_index
            .min(completions.len().saturating_sub(1));
        if let Some(choice) = completions.get(index) {
            self.input = choice.clone();
            self.completion_index = 0;
        }
    }

    fn command_completions(&self) -> Vec<String> {
        let mut candidates = vec![
            "help".to_string(),
            "refresh".to_string(),
            "ctx ".to_string(),
            "context ".to_string(),
            "cl ".to_string(),
            "cluster ".to_string(),
            "contexts".to_string(),
            "clusters".to_string(),
            "user ".to_string(),
            "usr ".to_string(),
            "users".to_string(),
            "all-ns".to_string(),
            "ns ".to_string(),
            "namespace ".to_string(),
            "namespaces".to_string(),
            "filter ".to_string(),
            "clear".to_string(),
            "logs".to_string(),
            "edit".to_string(),
            "delete".to_string(),
            "restart".to_string(),
            "scale ".to_string(),
            "exec ".to_string(),
            "shell".to_string(),
            "shell auto".to_string(),
            "shell /bin/sh".to_string(),
            "shell /bin/bash".to_string(),
            "bash".to_string(),
            "ssh".to_string(),
            "pf ".to_string(),
            "port-forward ".to_string(),
            "crd ".to_string(),
            "crd-refresh".to_string(),
            "q".to_string(),
        ];

        for tab in &self.tabs {
            candidates.push(tab.short_token().to_string());
            candidates.push(tab.title().to_ascii_lowercase());
        }

        if let Some(namespace_table) = self.tables.get(&ResourceTab::Namespaces) {
            for row in namespace_table.rows.iter().take(100) {
                candidates.push(format!("ns {}", row.name));
                candidates.push(format!("namespace {}", row.name));
                candidates.push(format!("namespaces {}", row.name));
            }
        }

        for context in self.available_contexts.iter().take(200) {
            candidates.push(format!("ctx {context}"));
            candidates.push(format!("context {context}"));
        }

        for cluster in self.available_clusters.iter().take(200) {
            candidates.push(format!("cl {cluster}"));
            candidates.push(format!("cluster {cluster}"));
        }

        for user in self.available_users.iter().take(200) {
            candidates.push(format!("user {user}"));
            candidates.push(format!("usr {user}"));
        }

        for crd in self.discovered_crds.iter().take(200) {
            candidates.push(format!("crd {}", crd.name));
            candidates.push(format!("crd {}", crd.kind.to_ascii_lowercase()));
            candidates.push(format!("crd {}", crd.plural));
        }

        filter_completions(candidates, &self.input, 200)
    }

    fn jump_completions(&self) -> Vec<String> {
        let mut candidates = vec![
            "ctx ".to_string(),
            "context ".to_string(),
            "cl ".to_string(),
            "cluster ".to_string(),
            "user ".to_string(),
            "usr ".to_string(),
            "contexts".to_string(),
            "clusters".to_string(),
            "users".to_string(),
        ];
        for tab in &self.tabs {
            candidates.push(tab.short_token().to_string());
            candidates.push(tab.title().to_ascii_lowercase());
        }

        for crd in self.discovered_crds.iter().take(100) {
            candidates.push(format!("crd {}", crd.name));
        }

        for context in self.available_contexts.iter().take(120) {
            candidates.push(format!("ctx {context}"));
            candidates.push(format!("context {context}"));
        }

        for cluster in self.available_clusters.iter().take(120) {
            candidates.push(format!("cl {cluster}"));
            candidates.push(format!("cluster {cluster}"));
        }

        for user in self.available_users.iter().take(120) {
            candidates.push(format!("user {user}"));
            candidates.push(format!("usr {user}"));
        }

        for tab in &self.tabs {
            if let Some(table) = self.tables.get(tab) {
                for row in table.rows.iter().take(60) {
                    if *tab == ResourceTab::Namespaces {
                        candidates.push(format!("{} {}", tab.short_token(), row.name));
                    } else if let Some(namespace) = row.namespace.as_deref() {
                        candidates.push(format!("{} {namespace}/{}", tab.short_token(), row.name));
                    } else {
                        candidates.push(format!("{} {}", tab.short_token(), row.name));
                    }
                }
            }
        }

        filter_completions(candidates, &self.input, 200)
    }

    fn selected_row_identity_for_tab(&self, tab: ResourceTab) -> Option<(Option<String>, String)> {
        let table = self.tables.get(&tab)?;
        let visible_rows = table
            .rows
            .iter()
            .filter(|row| row.matches_filter(&self.filter))
            .collect::<Vec<_>>();
        if visible_rows.is_empty() {
            return None;
        }

        let selected_index = table.selected.min(visible_rows.len().saturating_sub(1));
        let selected = visible_rows.get(selected_index)?;
        Some((selected.namespace.clone(), selected.name.clone()))
    }

    fn select_row_by_identity(&mut self, tab: ResourceTab, namespace: Option<String>, name: &str) {
        self.select_row_by_identity_with_fallback(tab, namespace, name, 0);
    }

    fn select_row_by_identity_with_fallback(
        &mut self,
        tab: ResourceTab,
        namespace: Option<String>,
        name: &str,
        fallback_selected: usize,
    ) {
        let filter = self.filter.clone();
        let Some(table) = self.tables.get_mut(&tab) else {
            return;
        };

        let mut visible_index = 0usize;
        let mut matched_indices = Vec::new();

        for row in &table.rows {
            if !row.matches_filter(&filter) {
                continue;
            }

            if row.name == name && row.namespace == namespace {
                matched_indices.push(visible_index);
            }

            visible_index = visible_index.saturating_add(1);
        }

        if !matched_indices.is_empty() {
            let index = matched_indices
                .into_iter()
                .min_by_key(|index| index.abs_diff(fallback_selected))
                .unwrap_or(0);
            table.selected = index;
        } else {
            let visible_len = table
                .rows
                .iter()
                .filter(|row| row.matches_filter(&filter))
                .count();
            table.selected = fallback_selected.min(visible_len.saturating_sub(1));
        }
    }

    fn selected_index_for_tab(&self, tab: ResourceTab) -> usize {
        self.tables
            .get(&tab)
            .map(|table| table.selected)
            .unwrap_or(0)
    }

    fn set_selected_index_for_tab(&mut self, tab: ResourceTab, selected: usize) {
        let filter = self.filter.clone();
        if let Some(table) = self.tables.get_mut(&tab) {
            let visible_len = table
                .rows
                .iter()
                .filter(|row| row.matches_filter(&filter))
                .count();
            table.selected = selected.min(visible_len.saturating_sub(1));
        }
    }

    fn enter_selected_resource(&mut self) -> AppCommand {
        if self.container_picker_active() {
            return self.load_selected_container_logs(false);
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No resource selected".to_string();
            return AppCommand::None;
        };

        let tab = self.active_tab();
        let row_name = row.name.clone();
        let row_namespace = row.namespace.clone();
        match tab {
            ResourceTab::Namespaces => {
                self.push_flow_state();
                let namespace = row_name;
                self.namespace_scope = NamespaceScope::Named(namespace.clone());
                self.filter.clear();
                self.clamp_all_selections();
                self.show_table_overview = false;
                self.clear_table_overlay();
                self.clear_detail_overlay();
                self.container_picker = None;
                self.detail_mode = DetailPaneMode::Dashboard;
                self.detail_scroll = 0;
                self.focus = FocusPane::Table;
                self.active_tab_index = self
                    .tabs
                    .iter()
                    .position(|entry| *entry == ResourceTab::Pods)
                    .unwrap_or(self.active_tab_index);
                self.status = format!("Entered namespace '{namespace}' (pods view)");
                AppCommand::RefreshAll
            }
            ResourceTab::Pods => {
                let Some(namespace) =
                    row.namespace
                        .clone()
                        .or_else(|| match self.namespace_scope() {
                            NamespaceScope::Named(ns) => Some(ns.clone()),
                            NamespaceScope::All => None,
                        })
                else {
                    self.status = "Pod namespace is unknown".to_string();
                    return AppCommand::None;
                };
                self.push_flow_state();
                let pod_name = row_name;
                self.status = format!("Loading containers for {namespace}/{pod_name}");
                AppCommand::LoadPodContainers {
                    namespace,
                    pod_name,
                }
            }
            ResourceTab::Deployments
            | ResourceTab::DaemonSets
            | ResourceTab::StatefulSets
            | ResourceTab::ReplicaSets
            | ResourceTab::ReplicationControllers
            | ResourceTab::Jobs
            | ResourceTab::CronJobs => {
                self.push_flow_state();
                self.drill_into_pods(row_namespace, &row_name, true)
            }
            ResourceTab::Services => {
                self.push_flow_state();
                self.drill_into_pods(row_namespace, &row_name, false)
            }
            _ => {
                self.status = format!(
                    "No enter drill-down for {} (press d for details)",
                    tab.title()
                );
                AppCommand::None
            }
        }
    }

    fn open_selected_details(&mut self) -> AppCommand {
        let Some(row) = self.active_selected_row() else {
            self.status = "No resource selected".to_string();
            return AppCommand::None;
        };

        let name = row.name.clone();
        self.show_table_overview = false;
        self.clear_table_overlay();
        self.container_picker = None;
        self.clear_detail_overlay();
        self.detail_mode = DetailPaneMode::Details;
        self.detail_scroll = 0;
        self.focus = FocusPane::Detail;
        self.status = format!("Opened details for {name}");
        AppCommand::None
    }

    fn drill_into_pods(
        &mut self,
        namespace: Option<String>,
        seed_filter: &str,
        use_seed_filter: bool,
    ) -> AppCommand {
        if let Some(namespace) = namespace {
            self.namespace_scope = NamespaceScope::Named(namespace);
        }
        self.filter = if use_seed_filter {
            seed_filter.to_string()
        } else {
            String::new()
        };
        self.show_table_overview = false;
        self.clear_table_overlay();
        self.clear_detail_overlay();
        self.container_picker = None;
        self.detail_mode = DetailPaneMode::Dashboard;
        self.focus = FocusPane::Table;
        self.clamp_all_selections();

        let switched = self.switch_to_tab(ResourceTab::Pods);
        self.status = "Drilled down to Pods".to_string();
        if switched == AppCommand::None {
            AppCommand::RefreshActive
        } else {
            switched
        }
    }

    fn submit_input(&mut self) -> AppCommand {
        match self.mode {
            InputMode::Normal => AppCommand::None,
            InputMode::Filter => {
                self.filter = self.input.trim().to_string();
                self.mode = InputMode::Normal;
                self.input.clear();
                self.completion_index = 0;
                self.clamp_all_selections();
                self.clear_detail_overlay();
                self.clear_table_overlay();

                if self.filter.is_empty() {
                    self.status = "Filter cleared".to_string();
                } else {
                    self.status = format!("Filter: '{}'", self.filter);
                }

                AppCommand::None
            }
            InputMode::Command => {
                let command = self.input.trim().to_string();
                self.mode = InputMode::Normal;
                self.input.clear();
                self.completion_index = 0;
                self.execute_command_line(&command)
            }
            InputMode::Jump => {
                let jump = self.input.trim().to_string();
                self.mode = InputMode::Normal;
                self.input.clear();
                self.completion_index = 0;
                self.execute_jump_line(&jump)
            }
        }
    }

    fn execute_command_line(&mut self, line: &str) -> AppCommand {
        let normalized = normalize_mode_prefixed_input(line);
        if normalized.is_empty() {
            self.status = "No command entered".to_string();
            return AppCommand::None;
        }
        self.reset_flow_root();

        let mut parts = normalized.split_whitespace();
        let command = resolve_command_token(parts.next().unwrap_or_default());

        match command.as_str() {
            "q" | "quit" | "exit" => {
                self.running = false;
                self.status = "Exit requested".to_string();
                AppCommand::None
            }
            "refresh" | "reload" | "r" => AppCommand::RefreshActive,
            "ctx" | "context" | "use-context" => {
                let Some(context) = parts.next() else {
                    self.status = "Usage: :ctx <context-name>".to_string();
                    return AppCommand::None;
                };
                self.status = format!("Switching context to '{context}'");
                AppCommand::SwitchContext {
                    context: context.to_string(),
                }
            }
            "cluster" | "cl" => {
                let Some(cluster) = parts.next() else {
                    self.status = "Usage: :cluster <cluster-name>".to_string();
                    return AppCommand::None;
                };
                self.status = format!("Switching cluster to '{cluster}'");
                AppCommand::SwitchCluster {
                    cluster: cluster.to_string(),
                }
            }
            "user" | "usr" => {
                let Some(user) = parts.next() else {
                    self.status = "Usage: :user <kubeconfig-user>".to_string();
                    return AppCommand::None;
                };
                self.status = format!("Switching to user '{user}'");
                AppCommand::SwitchUser {
                    user: user.to_string(),
                }
            }
            "contexts" => {
                self.status = format!(
                    "Contexts: {}",
                    format_catalog_preview(&self.available_contexts, 10)
                );
                AppCommand::None
            }
            "clusters" => {
                self.status = format!(
                    "Clusters: {}",
                    format_catalog_preview(&self.available_clusters, 10)
                );
                AppCommand::None
            }
            "users" => {
                self.status = format!(
                    "Users: {}",
                    format_catalog_preview(&self.available_users, 10)
                );
                AppCommand::None
            }
            "all-ns" | "allns" | "all" | "all-namespaces" => {
                self.namespace_scope = NamespaceScope::All;
                self.status = "Namespace scope set to all".to_string();
                AppCommand::RefreshAll
            }
            "ns" | "namespace" | "namespaces" => {
                if let Some(namespace) = parts.next() {
                    self.namespace_scope = NamespaceScope::Named(namespace.to_string());
                    self.status = format!("Namespace scope set to '{namespace}'");
                    AppCommand::RefreshAll
                } else {
                    self.switch_to_tab(ResourceTab::Namespaces)
                }
            }
            "tab" => {
                let Some(raw_tab) = parts.next() else {
                    self.status = "Usage: :tab <pods|deployments|services|...>".to_string();
                    return AppCommand::None;
                };

                let raw_tab = resolve_command_token(raw_tab);
                let Some(target_tab) = ResourceTab::from_token(&raw_tab) else {
                    self.status = format!("Unknown tab '{raw_tab}'");
                    return AppCommand::None;
                };

                let remainder = parts.collect::<Vec<_>>().join(" ");
                self.handle_tab_shortcut(target_tab, &remainder)
            }
            "filter" => {
                self.filter = parts.collect::<Vec<_>>().join(" ");
                self.clamp_all_selections();
                self.clear_detail_overlay();
                self.clear_table_overlay();
                if self.filter.is_empty() {
                    self.status = "Filter cleared".to_string();
                } else {
                    self.status = format!("Filter: '{}'", self.filter);
                }
                AppCommand::None
            }
            "clear" => {
                self.filter.clear();
                self.clamp_all_selections();
                self.clear_detail_overlay();
                self.clear_table_overlay();
                self.status = "Filter cleared".to_string();
                AppCommand::None
            }
            "logs" => self.create_logs_command(false),
            "edit" | "e" => self.prepare_edit_command(),
            "delete" | "del" => self.prepare_delete_confirmation(),
            "restart" => self.prepare_restart_confirmation(),
            "scale" => {
                let Some(raw_replicas) = parts.next() else {
                    self.status = "Usage: :scale <replicas>".to_string();
                    return AppCommand::None;
                };
                let Ok(replicas) = raw_replicas.parse::<i32>() else {
                    self.status = format!("Invalid replicas value '{raw_replicas}'");
                    return AppCommand::None;
                };
                self.prepare_scale_command(replicas)
            }
            "exec" => {
                let args = parts.map(|item| item.to_string()).collect::<Vec<_>>();
                self.prepare_exec_command(args)
            }
            "shell" | "ssh" => {
                let args = parts.map(|item| item.to_string()).collect::<Vec<_>>();
                let (container, shell) = parse_shell_args(args);
                self.prepare_shell_command(container, shell)
            }
            "bash" => self.prepare_shell_command(None, "/bin/bash".to_string()),
            "pf" | "port-forward" => {
                let Some(mapping) = parts.next() else {
                    self.status = "Usage: :port-forward <local>:<remote>".to_string();
                    return AppCommand::None;
                };
                let Some((local_port, remote_port)) = parse_port_mapping(mapping) else {
                    self.status = format!("Invalid port mapping '{mapping}'");
                    return AppCommand::None;
                };
                self.prepare_port_forward(local_port, remote_port)
            }
            "crd" | "custom" => self.select_custom_resource(parts.next()),
            "crd-refresh" => AppCommand::RefreshCustomResourceCatalog,
            "help" => {
                self.show_help = true;
                AppCommand::None
            }
            other => {
                if let Some(tab) = ResourceTab::from_token(other) {
                    let remainder = parts.collect::<Vec<_>>().join(" ");
                    return self.handle_tab_shortcut(tab, &remainder);
                }
                self.status = format!("Unknown command: {}", normalized);
                AppCommand::None
            }
        }
    }

    fn execute_jump_line(&mut self, line: &str) -> AppCommand {
        let normalized = normalize_mode_prefixed_input(line);
        let jump = normalized.as_str();
        if jump.is_empty() {
            self.status = "Jump query is empty".to_string();
            return AppCommand::None;
        }
        self.reset_flow_root();

        let mut parts = jump.split_whitespace();
        let first = resolve_command_token(parts.next().unwrap_or_default());
        if matches!(first.as_str(), "ctx" | "context") {
            let Some(context) = parts.next() else {
                self.status = "Usage: > ctx <context-name>".to_string();
                return AppCommand::None;
            };
            self.status = format!("Switching context to '{context}'");
            return AppCommand::SwitchContext {
                context: context.to_string(),
            };
        }

        if matches!(first.as_str(), "cluster" | "cl") {
            let Some(cluster) = parts.next() else {
                self.status = "Usage: > cluster <cluster-name>".to_string();
                return AppCommand::None;
            };
            self.status = format!("Switching cluster to '{cluster}'");
            return AppCommand::SwitchCluster {
                cluster: cluster.to_string(),
            };
        }

        if matches!(first.as_str(), "user" | "usr") {
            let Some(user) = parts.next() else {
                self.status = "Usage: > user <kubeconfig-user>".to_string();
                return AppCommand::None;
            };
            self.status = format!("Switching to user '{user}'");
            return AppCommand::SwitchUser {
                user: user.to_string(),
            };
        }

        if first == "contexts" {
            self.status = format!(
                "Contexts: {}",
                format_catalog_preview(&self.available_contexts, 10)
            );
            return AppCommand::None;
        }

        if first == "clusters" {
            self.status = format!(
                "Clusters: {}",
                format_catalog_preview(&self.available_clusters, 10)
            );
            return AppCommand::None;
        }

        if first == "users" {
            self.status = format!(
                "Users: {}",
                format_catalog_preview(&self.available_users, 10)
            );
            return AppCommand::None;
        }

        if let Some(tab) = ResourceTab::from_token(&first) {
            let remainder = parts.collect::<Vec<_>>().join(" ");
            return self.handle_tab_shortcut(tab, &remainder);
        }

        if first == "crd" {
            return self.select_custom_resource(parts.next());
        }

        let needle = jump.to_ascii_lowercase();
        for tab in self.tabs.clone() {
            let matched = self.tables.get(&tab).and_then(|table| {
                table
                    .rows
                    .iter()
                    .find(|row| {
                        row.name.to_ascii_lowercase().contains(&needle)
                            || row
                                .namespace
                                .as_ref()
                                .is_some_and(|ns| ns.to_ascii_lowercase().contains(&needle))
                    })
                    .map(|row| (row.namespace.clone(), row.name.clone()))
            });

            if let Some((namespace, name)) = matched {
                let command = self.switch_to_tab(tab);
                self.filter.clear();
                self.select_row_by_identity(tab, namespace, &name);
                self.status = format!("Jumped to {} {}", tab.title(), name);
                return command;
            }
        }

        self.status = format!("No resource matched jump query '{jump}'");
        AppCommand::None
    }

    fn handle_tab_shortcut(&mut self, tab: ResourceTab, remainder: &str) -> AppCommand {
        let remainder = remainder.trim();
        let command = self.switch_to_tab(tab);
        if remainder.is_empty() {
            return command;
        }

        if tab == ResourceTab::Namespaces {
            let namespace = parse_namespace_target(remainder);
            if namespace.is_empty() {
                self.status = "Namespace target is empty".to_string();
                return AppCommand::None;
            }

            self.namespace_scope = NamespaceScope::Named(namespace.clone());
            self.filter.clear();
            self.clamp_all_selections();
            self.status = format!("Namespace scope set to '{namespace}'");
            return AppCommand::RefreshAll;
        }

        if let Some((namespace, name)) = parse_namespaced_target(remainder) {
            self.filter.clear();
            self.select_row_by_identity(tab, Some(namespace.to_string()), &name);
            self.status = format!("Selected {} {}/{}", tab.title(), namespace, name);
            return command;
        }

        self.filter = remainder.to_string();
        self.clamp_all_selections();
        self.status = format!("Switched to {} with filter '{}'", tab.title(), remainder);
        command
    }

    fn select_custom_resource(&mut self, maybe_name: Option<&str>) -> AppCommand {
        if self.discovered_crds.is_empty() {
            self.status = "No CRDs discovered yet".to_string();
            return AppCommand::RefreshCustomResourceCatalog;
        }

        if let Some(name) = maybe_name {
            let needle = name.to_ascii_lowercase();
            let Some(found) = self.discovered_crds.iter().find(|crd| {
                crd.name.to_ascii_lowercase() == needle
                    || crd.kind.to_ascii_lowercase() == needle
                    || crd.plural.to_ascii_lowercase() == needle
            }) else {
                self.status = format!("CRD '{name}' was not found");
                return AppCommand::None;
            };
            self.selected_crd = Some(found.name.clone());
        }

        self.switch_to_tab(ResourceTab::CustomResources)
    }

    fn prepare_delete_confirmation(&mut self) -> AppCommand {
        let tab = self.active_tab();
        if matches!(tab, ResourceTab::Events | ResourceTab::CustomResources) {
            self.status = format!("Delete is not supported for {}", tab.title());
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected resource to delete".to_string();
            return AppCommand::None;
        };

        let namespace = match tab {
            ResourceTab::Nodes
            | ResourceTab::Namespaces
            | ResourceTab::IngressClasses
            | ResourceTab::StorageClasses
            | ResourceTab::PersistentVolumes
            | ResourceTab::ClusterRoles
            | ResourceTab::ClusterRoleBindings => None,
            _ => row.namespace.clone(),
        };
        let name = row.name.clone();
        let prompt = match &namespace {
            Some(ns) => format!("Delete {} {}/{}", tab.title(), ns, name),
            None => format!("Delete {} {}", tab.title(), name),
        };

        self.pending_confirmation = Some(PendingConfirmation {
            prompt: prompt.clone(),
            command: AppCommand::DeleteSelected {
                tab,
                namespace,
                name,
            },
        });
        self.status = format!("{prompt}? [y/n]");
        AppCommand::None
    }

    fn prepare_restart_confirmation(&mut self) -> AppCommand {
        let tab = self.active_tab();
        if !matches!(tab, ResourceTab::Deployments | ResourceTab::StatefulSets) {
            self.status = "Restart is available only for Deployments and StatefulSets".to_string();
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected workload".to_string();
            return AppCommand::None;
        };

        let Some(namespace) = row.namespace.clone() else {
            self.status = "Selected workload has no namespace".to_string();
            return AppCommand::None;
        };
        let name = row.name.clone();
        let prompt = format!("Restart {} {}/{}", tab.title(), namespace, name);
        self.pending_confirmation = Some(PendingConfirmation {
            prompt: prompt.clone(),
            command: AppCommand::RestartWorkload {
                tab,
                namespace,
                name,
            },
        });
        self.status = format!("{prompt}? [y/n]");
        AppCommand::None
    }

    fn prepare_scale_command(&mut self, replicas: i32) -> AppCommand {
        if replicas < 0 {
            self.status = "Replicas must be >= 0".to_string();
            return AppCommand::None;
        }

        let tab = self.active_tab();
        if !matches!(tab, ResourceTab::Deployments | ResourceTab::StatefulSets) {
            self.status = "Scale is available only for Deployments and StatefulSets".to_string();
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected workload".to_string();
            return AppCommand::None;
        };

        let Some(namespace) = row.namespace.clone() else {
            self.status = "Selected workload has no namespace".to_string();
            return AppCommand::None;
        };
        let name = row.name.clone();
        self.status = format!(
            "Scaling {} {}/{} to {} replicas",
            tab.title(),
            namespace,
            name,
            replicas
        );
        AppCommand::ScaleWorkload {
            tab,
            namespace,
            name,
            replicas,
        }
    }

    fn prepare_exec_command(&mut self, command: Vec<String>) -> AppCommand {
        if self.active_tab() != ResourceTab::Pods {
            self.status = "Exec is only available in the Pods tab".to_string();
            return AppCommand::None;
        }

        if command.is_empty() {
            self.status = "Usage: :exec <command...>".to_string();
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected pod".to_string();
            return AppCommand::None;
        };
        let Some(namespace) = row.namespace.clone() else {
            self.status = "Selected pod has no namespace".to_string();
            return AppCommand::None;
        };
        let pod_name = row.name.clone();
        self.status = format!("Executing in {namespace}/{pod_name}: {}", command.join(" "));
        AppCommand::ExecInPod {
            namespace,
            pod_name,
            command,
        }
    }

    fn prepare_shell_command(&mut self, container: Option<String>, shell: String) -> AppCommand {
        if self.active_tab() != ResourceTab::Pods {
            self.status = "Shell access is only available from the Pods tab".to_string();
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected pod".to_string();
            return AppCommand::None;
        };
        let Some(namespace) = row.namespace.clone() else {
            self.status = "Selected pod has no namespace".to_string();
            return AppCommand::None;
        };
        let pod_name = row.name.clone();
        self.status = match container.as_deref() {
            Some(container) => format!(
                "Opening shell in {namespace}/{pod_name} (container: {container}, shell: {shell})"
            ),
            None => format!("Opening shell in {namespace}/{pod_name} ({shell})"),
        };
        AppCommand::OpenPodShell {
            namespace,
            pod_name,
            container,
            shell,
        }
    }

    fn prepare_edit_command(&mut self) -> AppCommand {
        let tab = self.active_tab();
        let Some((resource, namespaced)) = self.kubectl_resource_for_tab(tab) else {
            self.status = format!("Edit is not supported for {}", tab.title());
            return AppCommand::None;
        };

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected resource".to_string();
            return AppCommand::None;
        };

        let name = row.name.clone();
        let namespace = if namespaced {
            row.namespace
                .clone()
                .or_else(|| match self.namespace_scope() {
                    NamespaceScope::Named(namespace) => Some(namespace.clone()),
                    NamespaceScope::All => None,
                })
        } else {
            None
        };

        if namespaced && namespace.is_none() {
            self.status = "Selected resource has no namespace".to_string();
            return AppCommand::None;
        }

        self.status = match namespace.as_deref() {
            Some(namespace) => format!("Editing {resource} {namespace}/{name}"),
            None => format!("Editing {resource} {name}"),
        };

        AppCommand::EditSelected {
            resource,
            namespace,
            name,
        }
    }

    fn kubectl_resource_for_tab(&self, tab: ResourceTab) -> Option<(String, bool)> {
        match tab {
            ResourceTab::Pods => Some(("pod".to_string(), true)),
            ResourceTab::CronJobs => Some(("cronjob".to_string(), true)),
            ResourceTab::DaemonSets => Some(("daemonset".to_string(), true)),
            ResourceTab::Deployments => Some(("deployment".to_string(), true)),
            ResourceTab::ReplicaSets => Some(("replicaset".to_string(), true)),
            ResourceTab::ReplicationControllers => {
                Some(("replicationcontroller".to_string(), true))
            }
            ResourceTab::StatefulSets => Some(("statefulset".to_string(), true)),
            ResourceTab::Jobs => Some(("job".to_string(), true)),
            ResourceTab::Services => Some(("service".to_string(), true)),
            ResourceTab::Ingresses => Some(("ingress".to_string(), true)),
            ResourceTab::IngressClasses => Some(("ingressclass".to_string(), false)),
            ResourceTab::ConfigMaps => Some(("configmap".to_string(), true)),
            ResourceTab::PersistentVolumeClaims => {
                Some(("persistentvolumeclaim".to_string(), true))
            }
            ResourceTab::Secrets => Some(("secret".to_string(), true)),
            ResourceTab::StorageClasses => Some(("storageclass".to_string(), false)),
            ResourceTab::PersistentVolumes => Some(("persistentvolume".to_string(), false)),
            ResourceTab::ServiceAccounts => Some(("serviceaccount".to_string(), true)),
            ResourceTab::Roles => Some(("role".to_string(), true)),
            ResourceTab::RoleBindings => Some(("rolebinding".to_string(), true)),
            ResourceTab::ClusterRoles => Some(("clusterrole".to_string(), false)),
            ResourceTab::ClusterRoleBindings => Some(("clusterrolebinding".to_string(), false)),
            ResourceTab::NetworkPolicies => Some(("networkpolicy".to_string(), true)),
            ResourceTab::Nodes => Some(("node".to_string(), false)),
            ResourceTab::Namespaces => Some(("namespace".to_string(), false)),
            ResourceTab::Events => None,
            ResourceTab::CustomResources => {
                let crd = self.selected_custom_resource()?;
                let resource = if crd.group.is_empty() {
                    crd.plural.clone()
                } else {
                    format!("{}.{}", crd.plural, crd.group)
                };
                Some((resource, crd.namespaced))
            }
        }
    }

    fn prepare_port_forward(&mut self, local_port: u16, remote_port: u16) -> AppCommand {
        let tab = self.active_tab();
        if !matches!(tab, ResourceTab::Pods | ResourceTab::Services) {
            self.status = "Port-forward is available in Pods and Services tabs".to_string();
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected target for port-forward".to_string();
            return AppCommand::None;
        };
        let Some(namespace) = row.namespace.clone() else {
            self.status = "Selected target has no namespace".to_string();
            return AppCommand::None;
        };
        let name = row.name.clone();
        self.status = format!(
            "Starting port-forward {} {}/{} {}:{}",
            tab.title(),
            namespace,
            name,
            local_port,
            remote_port
        );
        AppCommand::StartPortForward {
            tab,
            namespace,
            name,
            local_port,
            remote_port,
        }
    }

    fn create_logs_command(&mut self, previous: bool) -> AppCommand {
        if self.container_picker_active() {
            return self.load_selected_container_logs(previous);
        }

        if self.active_tab() != ResourceTab::Pods {
            self.status =
                "Logs are available from Pods (or use Shift+L for workload logs)".to_string();
            return AppCommand::None;
        }

        let Some(selected_row) = self.active_selected_row() else {
            self.status = "No pod selected".to_string();
            return AppCommand::None;
        };

        let Some(namespace) =
            selected_row
                .namespace
                .clone()
                .or_else(|| match self.namespace_scope() {
                    NamespaceScope::All => None,
                    NamespaceScope::Named(ns) => Some(ns.clone()),
                })
        else {
            self.status = "Pod namespace is unknown".to_string();
            return AppCommand::None;
        };

        let pod_name = selected_row.name.clone();
        self.status = if previous {
            format!("Fetching previous logs for pod '{pod_name}' in '{namespace}'")
        } else {
            format!("Fetching logs for pod '{pod_name}' in '{namespace}'")
        };

        AppCommand::LoadPodLogs {
            namespace,
            pod_name,
            container: None,
            previous,
        }
    }

    fn create_related_logs_command(&mut self, previous: bool) -> AppCommand {
        if self.container_picker_active() {
            return self.load_selected_container_logs(previous);
        }

        let tab = self.active_tab();
        if tab == ResourceTab::Pods {
            return self.create_logs_command(previous);
        }

        if !supports_related_logs(tab) {
            self.status = format!("Shift+L logs are not supported for {}", tab.title());
            return AppCommand::None;
        }

        let Some(row) = self.active_selected_row() else {
            self.status = "No selected resource".to_string();
            return AppCommand::None;
        };
        let name = row.name.clone();
        let namespace = row.namespace.clone();
        self.status = format!("Resolving related logs for {name}");
        AppCommand::LoadResourceLogs {
            tab,
            namespace,
            name,
            previous,
        }
    }

    fn load_selected_container_logs(&mut self, previous: bool) -> AppCommand {
        let Some(picker) = self.container_picker.as_ref() else {
            self.status = "No container selected".to_string();
            return AppCommand::None;
        };
        if picker.containers.is_empty() {
            self.status = "No containers available".to_string();
            return AppCommand::None;
        }
        let selected = picker
            .selected
            .min(picker.containers.len().saturating_sub(1));
        let container = picker.containers[selected].name.clone();
        self.status = if previous {
            format!(
                "Fetching previous logs for {}/{} container '{}'",
                picker.namespace, picker.pod_name, container
            )
        } else {
            format!(
                "Fetching logs for {}/{} container '{}'",
                picker.namespace, picker.pod_name, container
            )
        };
        AppCommand::LoadPodLogs {
            namespace: picker.namespace.clone(),
            pod_name: picker.pod_name.clone(),
            container: Some(container),
            previous,
        }
    }

    fn clear_detail_overlay(&mut self) {
        self.detail_overlay_title = None;
        self.detail_overlay = None;
    }

    fn clear_table_overlay(&mut self) {
        self.table_overlay_title = None;
        self.table_overlay = None;
        self.table_overlay_kind = TableOverlayKind::Generic;
        self.table_overlay_return_picker = None;
        self.table_scroll = 0;
    }

    fn clear_container_picker(&mut self) {
        self.container_picker = None;
    }

    fn dismiss_detail_view(&mut self) {
        self.clear_detail_overlay();
        self.detail_mode = DetailPaneMode::Dashboard;
        self.detail_scroll = 0;
        self.focus = FocusPane::Table;
    }

    fn table_page_step(&self) -> isize {
        self.table_page_size.saturating_sub(1).max(1) as isize
    }

    fn detail_page_step(&self) -> u16 {
        self.detail_view_height.saturating_div(2).max(1)
    }

    fn scroll_detail(&mut self, delta: isize) {
        let max = self.detail_max_scroll() as isize;
        let current = self.detail_scroll as isize;
        let next = (current + delta).clamp(0, max);
        self.detail_scroll = next as u16;
    }

    fn scroll_table_overlay(&mut self, delta: isize) {
        let max = self.table_max_scroll() as isize;
        let current = self.table_scroll as isize;
        let next = (current + delta).clamp(0, max);
        self.table_scroll = next as u16;
    }

    fn detail_max_scroll(&self) -> u16 {
        let width = self.detail_view_width.max(1) as usize;
        let height = self.detail_view_height.max(1) as usize;
        let text = if let Some(overlay) = &self.detail_overlay {
            overlay.as_str()
        } else {
            self.active_selected_row()
                .map(|row| row.detail.as_str())
                .unwrap_or("No resource selected")
        };

        let visual_lines = visual_line_count(text, width);
        visual_lines.saturating_sub(height) as u16
    }

    fn table_max_scroll(&self) -> u16 {
        let width = self.table_view_width.max(1) as usize;
        let height = self.table_view_height.max(1) as usize;
        let text = self.table_overlay.as_deref().unwrap_or("");
        let visual_lines = visual_line_count(text, width);
        visual_lines.saturating_sub(height) as u16
    }
}

fn visual_line_count(text: &str, width: usize) -> usize {
    let width = width.max(1);
    text.lines()
        .map(|line| {
            let chars = line.chars().count();
            chars.div_ceil(width).max(1)
        })
        .sum::<usize>()
        .max(1)
}

fn parse_port_mapping(mapping: &str) -> Option<(u16, u16)> {
    let mut parts = mapping.split(':');
    let local = parts.next()?.parse::<u16>().ok()?;
    let remote = parts.next()?.parse::<u16>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((local, remote))
}

fn resolve_command_token(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    let aliases = lower
        .split(':')
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .collect::<Vec<_>>();

    if aliases.is_empty() {
        return String::new();
    }

    for alias in &aliases {
        if is_known_command_token(alias) {
            return (*alias).to_string();
        }
    }

    aliases[0].to_string()
}

fn is_known_command_token(token: &str) -> bool {
    matches!(
        token,
        "q" | "quit"
            | "exit"
            | "refresh"
            | "reload"
            | "r"
            | "ctx"
            | "context"
            | "use-context"
            | "cluster"
            | "cl"
            | "user"
            | "usr"
            | "contexts"
            | "clusters"
            | "users"
            | "all-ns"
            | "allns"
            | "all"
            | "all-namespaces"
            | "ns"
            | "namespace"
            | "namespaces"
            | "tab"
            | "filter"
            | "clear"
            | "logs"
            | "edit"
            | "e"
            | "delete"
            | "del"
            | "restart"
            | "scale"
            | "exec"
            | "shell"
            | "ssh"
            | "bash"
            | "pf"
            | "port-forward"
            | "crd"
            | "custom"
            | "crd-refresh"
            | "help"
    ) || ResourceTab::from_token(token).is_some()
}

fn supports_related_logs(tab: ResourceTab) -> bool {
    matches!(
        tab,
        ResourceTab::Pods
            | ResourceTab::Deployments
            | ResourceTab::DaemonSets
            | ResourceTab::StatefulSets
            | ResourceTab::ReplicaSets
            | ResourceTab::ReplicationControllers
            | ResourceTab::Jobs
            | ResourceTab::CronJobs
            | ResourceTab::Services
    )
}

fn parse_namespaced_target(input: &str) -> Option<(&str, String)> {
    let (namespace, name) = input.split_once('/')?;
    let namespace = namespace.trim();
    let name = name.trim();
    if namespace.is_empty() || name.is_empty() {
        return None;
    }
    Some((namespace, name.to_string()))
}

fn parse_namespace_target(input: &str) -> String {
    if let Some((_, name)) = parse_namespaced_target(input) {
        return name;
    }
    input.trim().to_string()
}

fn parse_shell_args(args: Vec<String>) -> (Option<String>, String) {
    match args.as_slice() {
        [] => (None, "auto".to_string()),
        [single] => {
            if is_shell_token(single) {
                (None, normalize_shell_token(single))
            } else {
                (Some(single.clone()), "auto".to_string())
            }
        }
        [container, shell, ..] => (Some(container.clone()), normalize_shell_token(shell)),
    }
}

fn is_shell_token(token: &str) -> bool {
    matches!(token, "sh" | "bash" | "auto") || token.starts_with('/')
}

fn normalize_shell_token(token: &str) -> String {
    match token {
        "sh" => "/bin/sh".to_string(),
        "bash" => "/bin/bash".to_string(),
        "auto" => "auto".to_string(),
        _ => token.to_string(),
    }
}

fn filter_completions(mut candidates: Vec<String>, input: &str, limit: usize) -> Vec<String> {
    candidates.sort();
    candidates.dedup();

    let query = normalize_mode_prefixed_input(input).to_ascii_lowercase();
    if !query.is_empty() {
        candidates = candidates
            .into_iter()
            .filter(|candidate| completion_matches(candidate, &query))
            .collect::<Vec<_>>();
    }

    candidates.truncate(limit);
    candidates
}

fn normalize_mode_prefixed_input(input: &str) -> String {
    let mut query = input.trim();
    while let Some(stripped) = query.strip_prefix(':').or_else(|| query.strip_prefix('>')) {
        query = stripped.trim_start();
    }
    query.to_string()
}

fn format_catalog_preview(values: &[String], limit: usize) -> String {
    if values.is_empty() {
        return "-".to_string();
    }

    let shown = values
        .iter()
        .take(limit)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if values.len() > limit {
        format!("{shown}, +{}", values.len().saturating_sub(limit))
    } else {
        shown
    }
}

fn completion_matches(candidate: &str, query: &str) -> bool {
    let lower = candidate.to_ascii_lowercase();
    if lower.starts_with(query) {
        return true;
    }

    let words = lower
        .split(|ch: char| ch.is_ascii_whitespace() || matches!(ch, '/' | ':' | '-' | '.'))
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    query
        .split_whitespace()
        .all(|token| words.iter().any(|word| word.starts_with(token)))
}

fn summarize_error_line(error: &str) -> String {
    error
        .lines()
        .find(|line| !line.trim().is_empty())
        .map(|line| line.trim().to_string())
        .unwrap_or_else(|| "unknown error".to_string())
}

fn normalize_status_text(status: String) -> String {
    const MAX_STATUS_LEN: usize = 180;
    if status.chars().count() <= MAX_STATUS_LEN {
        return status;
    }

    let mut shortened = status
        .chars()
        .take(MAX_STATUS_LEN.saturating_sub(1))
        .collect::<String>();
    shortened.push('…');
    shortened
}

#[cfg(test)]
mod tests {
    use super::{App, AppCommand, DetailPaneMode, normalize_mode_prefixed_input};
    use crate::input::Action;
    use crate::model::{NamespaceScope, ResourceTab, RowData, TableData};
    use chrono::Local;

    #[test]
    fn filter_command_sets_filter() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "filter api".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(cmd, AppCommand::None);
        assert_eq!(app.filter(), "api");
    }

    #[test]
    fn ns_command_requests_refresh_all() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "ns kube-system".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(cmd, AppCommand::RefreshAll);
        assert_eq!(
            app.namespace_scope(),
            &NamespaceScope::Named("kube-system".to_string())
        );
    }

    #[test]
    fn ns_without_arg_switches_to_namespaces_tab() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "ns".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let _ = app.apply_action(Action::SubmitInput);
        assert_eq!(app.active_tab(), ResourceTab::Namespaces);
    }

    #[test]
    fn bare_tab_token_switches_tab() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "deployments".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let _ = app.apply_action(Action::SubmitInput);
        assert_eq!(app.active_tab(), ResourceTab::Deployments);
    }

    #[test]
    fn namespace_alias_token_sets_scope() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "namespace:ns kube-system".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(cmd, AppCommand::RefreshAll);
        assert_eq!(
            app.namespace_scope(),
            &NamespaceScope::Named("kube-system".to_string())
        );
    }

    #[test]
    fn scale_command_executes_without_confirmation() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut deployments = TableData::default();
        deployments.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "web".to_string(),
                namespace: Some("orca-sandbox".to_string()),
                columns: vec!["web".to_string()],
                detail: "kind: Deployment".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Deployments, deployments);
        let _ = app.switch_to_tab(ResourceTab::Deployments);

        app.apply_action(Action::StartCommand);
        for c in "scale 2".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::ScaleWorkload {
                tab: ResourceTab::Deployments,
                namespace: "orca-sandbox".to_string(),
                name: "web".to_string(),
                replicas: 2
            }
        );
        assert!(app.pending_confirmation_prompt().is_none());
    }

    #[test]
    fn prefixed_scale_command_executes_without_confirmation() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut deployments = TableData::default();
        deployments.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "web".to_string(),
                namespace: Some("orca-sandbox".to_string()),
                columns: vec!["web".to_string()],
                detail: "kind: Deployment".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Deployments, deployments);
        let _ = app.switch_to_tab(ResourceTab::Deployments);

        app.apply_action(Action::StartCommand);
        for c in ":scale 2".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::ScaleWorkload {
                tab: ResourceTab::Deployments,
                namespace: "orca-sandbox".to_string(),
                name: "web".to_string(),
                replicas: 2
            }
        );
        assert!(app.pending_confirmation_prompt().is_none());
    }

    #[test]
    fn command_completion_empty_does_not_block_submission() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        app.apply_action(Action::StartCommand);
        for c in ":scale 999".chars() {
            app.apply_action(Action::InputChar(c));
        }
        // Ensure parse/submit still runs even if completion UI has no candidates.
        let _ = app.completion_candidates();
        let cmd = app.apply_action(Action::SubmitInput);
        assert!(matches!(
            cmd,
            AppCommand::ScaleWorkload { replicas: 999, .. } | AppCommand::None
        ));
    }

    #[test]
    fn jump_namespace_path_sets_scope_and_refreshes() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartJump);
        for c in "ns openclaw/openclaw".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(cmd, AppCommand::RefreshAll);
        assert_eq!(
            app.namespace_scope(),
            &NamespaceScope::Named("openclaw".to_string())
        );
        assert_eq!(app.filter(), "");
    }

    #[test]
    fn ctx_command_returns_switch_context_command() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "ctx dev-cluster".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::SwitchContext {
                context: "dev-cluster".to_string()
            }
        );
    }

    #[test]
    fn prefixed_ctx_command_returns_switch_context_command() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in ":context dev-cluster".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::SwitchContext {
                context: "dev-cluster".to_string()
            }
        );
    }

    #[test]
    fn jump_cluster_returns_switch_cluster_command() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartJump);
        for c in "cluster homelab".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::SwitchCluster {
                cluster: "homelab".to_string()
            }
        );
    }

    #[test]
    fn prefixed_jump_cluster_returns_switch_cluster_command() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartJump);
        for c in ">cluster homelab".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::SwitchCluster {
                cluster: "homelab".to_string()
            }
        );
    }

    #[test]
    fn user_command_returns_switch_user_command() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "user platform-admin".chars() {
            app.apply_action(Action::InputChar(c));
        }

        let cmd = app.apply_action(Action::SubmitInput);
        assert_eq!(
            cmd,
            AppCommand::SwitchUser {
                user: "platform-admin".to_string()
            }
        );
    }

    #[test]
    fn completion_query_normalizes_mode_prefix() {
        assert_eq!(normalize_mode_prefixed_input(":context"), "context");
        assert_eq!(
            normalize_mode_prefixed_input(">cluster home"),
            "cluster home"
        );
    }

    #[test]
    fn command_completion_excludes_legacy_tab_prefix() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        app.apply_action(Action::StartCommand);
        let completions = app.completion_candidates();
        assert!(
            !completions
                .iter()
                .any(|candidate| candidate.starts_with("tab ")),
            "legacy tab-prefix completions should be hidden"
        );
    }

    #[test]
    fn enter_resource_on_pod_requests_container_list() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        let now = Local::now();
        let mut data = TableData::default();
        data.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "pod-1".to_string(),
                namespace: Some("default".to_string()),
                columns: vec!["pod-1".to_string()],
                detail: "kind: Pod".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Pods, data);

        let cmd = app.apply_action(Action::EnterResource);
        assert_eq!(
            cmd,
            AppCommand::LoadPodContainers {
                namespace: "default".to_string(),
                pod_name: "pod-1".to_string()
            }
        );
    }

    #[test]
    fn esc_returns_to_dashboard_mode() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut data = TableData::default();
        data.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "pod-1".to_string(),
                namespace: Some("default".to_string()),
                columns: vec!["pod-1".to_string()],
                detail: "kind: Pod".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Pods, data);

        let _ = app.apply_action(Action::ShowDetails);
        assert_eq!(app.detail_mode(), DetailPaneMode::Details);
        assert_eq!(app.pane_label(), "det");

        let _ = app.apply_action(Action::ClearDetailOverlay);
        assert_eq!(app.detail_mode(), DetailPaneMode::Dashboard);
        assert_eq!(app.pane_label(), "tbl");
    }

    #[test]
    fn esc_from_container_logs_returns_to_container_picker_first() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut data = TableData::default();
        data.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "pod-1".to_string(),
                namespace: Some("default".to_string()),
                columns: vec!["pod-1".to_string()],
                detail: "kind: Pod".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Pods, data);
        app.set_container_picker(
            "default",
            "pod-1",
            vec![crate::model::PodContainerInfo {
                name: "c1".to_string(),
                image: "img:v1".to_string(),
                ready: true,
                state: "Running".to_string(),
                restarts: 0,
                age: "1m".to_string(),
            }],
        );
        assert!(app.container_picker_active());

        app.set_pod_logs_overlay("Pod Logs default/pod-1:c1", "line".to_string());
        assert!(app.table_overlay_active());
        assert!(!app.container_picker_active());
        assert_eq!(app.pane_label(), "log");

        let _ = app.apply_action(Action::ClearDetailOverlay);
        assert!(app.container_picker_active());
        assert!(!app.table_overlay_active());
        assert_eq!(app.pane_label(), "ctr");

        let _ = app.apply_action(Action::ClearDetailOverlay);
        assert!(!app.container_picker_active());
        assert_eq!(app.pane_label(), "tbl");
    }

    #[test]
    fn pane_label_uses_uppercase_for_related_logs() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        app.set_related_logs_overlay("Logs default/pod-1", "line".to_string());
        assert_eq!(app.pane_label(), "LOG");
    }

    #[test]
    fn pane_label_uses_shell_for_embedded_shell_overlay() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        app.set_shell_overlay("Pod Shell", "# echo hello\nhello\n".to_string());
        assert_eq!(app.pane_label(), "sh");
        assert!(app.shell_overlay_active());
    }

    #[test]
    fn enter_namespace_drills_into_pods_scope() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut namespaces = TableData::default();
        namespaces.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "orca-sandbox".to_string(),
                namespace: Some("orca-sandbox".to_string()),
                columns: vec!["orca-sandbox".to_string()],
                detail: "kind: Namespace".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Namespaces, namespaces);
        app.switch_to_tab(ResourceTab::Namespaces);

        let cmd = app.apply_action(Action::EnterResource);
        assert_eq!(cmd, AppCommand::RefreshAll);
        assert_eq!(app.active_tab(), ResourceTab::Pods);
        assert_eq!(
            app.namespace_scope(),
            &NamespaceScope::Named("orca-sandbox".to_string())
        );
    }

    #[test]
    fn esc_returns_to_command_root_after_drilldown() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();

        let mut deployments = TableData::default();
        deployments.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "web".to_string(),
                namespace: Some("openclaw".to_string()),
                columns: vec!["web".to_string()],
                detail: "kind: Deployment".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Deployments, deployments);

        app.apply_action(Action::StartCommand);
        for c in "deploy".chars() {
            app.apply_action(Action::InputChar(c));
        }
        let _ = app.apply_action(Action::SubmitInput);
        assert_eq!(app.active_tab(), ResourceTab::Deployments);

        let cmd = app.apply_action(Action::EnterResource);
        assert_eq!(cmd, AppCommand::RefreshActive);
        assert_eq!(app.active_tab(), ResourceTab::Pods);

        let _ = app.apply_action(Action::ClearDetailOverlay);
        assert_eq!(app.active_tab(), ResourceTab::Deployments);
    }

    #[test]
    fn shift_l_on_workload_builds_related_logs_command() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut data = TableData::default();
        data.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "openclaw-ag".to_string(),
                namespace: Some("openclaw".to_string()),
                columns: vec!["openclaw-ag".to_string()],
                detail: "kind: Deployment".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Deployments, data);
        app.switch_to_tab(ResourceTab::Deployments);

        let cmd = app.apply_action(Action::LoadResourceLogs);
        assert_eq!(
            cmd,
            AppCommand::LoadResourceLogs {
                tab: ResourceTab::Deployments,
                namespace: Some("openclaw".to_string()),
                name: "openclaw-ag".to_string(),
                previous: true
            }
        );
    }

    #[test]
    fn moving_selection_keeps_logs_overlay_open() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();
        let mut data = TableData::default();
        data.set_rows(
            vec!["Name".to_string()],
            vec![
                RowData {
                    name: "pod-1".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["pod-1".to_string()],
                    detail: "kind: Pod".to_string(),
                },
                RowData {
                    name: "pod-2".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["pod-2".to_string()],
                    detail: "kind: Pod".to_string(),
                },
            ],
            now,
        );
        app.set_active_table_data(ResourceTab::Pods, data);
        app.set_detail_overlay("Pod Logs", "line".to_string());
        let _ = app.apply_action(Action::ToggleFocus);
        let _ = app.apply_action(Action::Down);

        assert!(app.detail_overlay_active());
        assert_eq!(app.active_selected_index(), Some(1));
    }

    #[test]
    fn switching_tabs_keeps_state_consistent() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        let now = Local::now();
        let mut data = TableData::default();
        data.set_rows(
            vec!["Name".to_string()],
            vec![RowData {
                name: "pod-1".to_string(),
                namespace: Some("default".to_string()),
                columns: vec!["pod-1".to_string()],
                detail: "detail".to_string(),
            }],
            now,
        );
        app.set_active_table_data(ResourceTab::Pods, data);

        let _ = app.apply_action(Action::NextTab);
        let _ = app.apply_action(Action::PrevTab);
        assert_eq!(app.active_tab(), ResourceTab::Pods);
    }

    #[test]
    fn switching_view_slots_preserves_state_per_slot() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        app.apply_action(Action::StartCommand);
        for c in "deployments".chars() {
            app.apply_action(Action::InputChar(c));
        }
        let _ = app.apply_action(Action::SubmitInput);

        app.apply_action(Action::StartCommand);
        for c in "filter web".chars() {
            app.apply_action(Action::InputChar(c));
        }
        let _ = app.apply_action(Action::SubmitInput);

        let _ = app.apply_action(Action::SwitchView(1));
        assert_eq!(app.active_view_slot(), 1);
        assert!(app.view_slot_initialized(1));

        let _ = app.apply_action(Action::SwitchView(2));
        assert_eq!(app.active_view_slot(), 2);
        assert!(app.view_slot_initialized(2));

        app.apply_action(Action::StartCommand);
        for c in "pods".chars() {
            app.apply_action(Action::InputChar(c));
        }
        let _ = app.apply_action(Action::SubmitInput);
        assert_eq!(app.active_tab(), ResourceTab::Pods);

        let _ = app.apply_action(Action::SwitchView(1));
        assert_eq!(app.active_view_slot(), 1);
        assert_eq!(app.active_tab(), ResourceTab::Deployments);
        assert_eq!(app.filter(), "web");
    }

    #[test]
    fn deleting_inactive_view_slot_clears_it() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        let _ = app.apply_action(Action::SwitchView(2));
        assert_eq!(app.active_view_slot(), 2);
        assert!(app.view_slot_initialized(1));
        assert!(app.view_slot_initialized(2));

        let cmd = app.apply_action(Action::DeleteView(1));
        assert_eq!(cmd, AppCommand::None);
        assert_eq!(app.active_view_slot(), 2);
        assert!(!app.view_slot_initialized(1));
        assert!(app.view_slot_initialized(2));
    }

    #[test]
    fn deleting_active_view_slot_switches_to_fallback() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        let _ = app.apply_action(Action::SwitchView(2));
        assert_eq!(app.active_view_slot(), 2);

        let cmd = app.apply_action(Action::DeleteView(2));
        assert_eq!(cmd, AppCommand::RefreshActive);
        assert_eq!(app.active_view_slot(), 1);
        assert!(!app.view_slot_initialized(2));
        assert!(app.view_slot_initialized(1));
    }

    #[test]
    fn deleting_last_active_view_is_rejected() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );

        let cmd = app.apply_action(Action::DeleteView(1));
        assert_eq!(cmd, AppCommand::None);
        assert_eq!(app.active_view_slot(), 1);
        assert!(app.view_slot_initialized(1));
    }

    #[test]
    fn refresh_keeps_previous_index_when_identity_disappears() {
        let mut app = App::new(
            "cluster".to_string(),
            "context".to_string(),
            NamespaceScope::Named("default".to_string()),
        );
        let now = Local::now();

        let mut initial = TableData::default();
        initial.set_rows(
            vec!["Name".to_string()],
            vec![
                RowData {
                    name: "a".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["a".to_string()],
                    detail: "a".to_string(),
                },
                RowData {
                    name: "b".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["b".to_string()],
                    detail: "b".to_string(),
                },
                RowData {
                    name: "c".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["c".to_string()],
                    detail: "c".to_string(),
                },
            ],
            now,
        );
        app.set_active_table_data(ResourceTab::Pods, initial);
        let _ = app.apply_action(Action::Down);
        let _ = app.apply_action(Action::Down);
        assert_eq!(app.active_selected_index(), Some(2));

        let mut refreshed = TableData::default();
        refreshed.set_rows(
            vec!["Name".to_string()],
            vec![
                RowData {
                    name: "x".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["x".to_string()],
                    detail: "x".to_string(),
                },
                RowData {
                    name: "y".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["y".to_string()],
                    detail: "y".to_string(),
                },
                RowData {
                    name: "z".to_string(),
                    namespace: Some("default".to_string()),
                    columns: vec!["z".to_string()],
                    detail: "z".to_string(),
                },
            ],
            Local::now(),
        );
        app.set_active_table_data(ResourceTab::Pods, refreshed);

        assert_eq!(app.active_selected_index(), Some(2));
    }
}
