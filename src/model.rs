use chrono::{DateTime, Local};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub enum ResourceTab {
    Pods,
    CronJobs,
    DaemonSets,
    Deployments,
    ReplicaSets,
    ReplicationControllers,
    StatefulSets,
    Jobs,
    Services,
    Ingresses,
    IngressClasses,
    ConfigMaps,
    PersistentVolumeClaims,
    Secrets,
    StorageClasses,
    PersistentVolumes,
    ServiceAccounts,
    Roles,
    RoleBindings,
    ClusterRoles,
    ClusterRoleBindings,
    NetworkPolicies,
    Nodes,
    Events,
    Namespaces,
    CustomResources,
}

impl ResourceTab {
    pub const ALL: [Self; 26] = [
        Self::Pods,
        Self::CronJobs,
        Self::DaemonSets,
        Self::Deployments,
        Self::ReplicaSets,
        Self::ReplicationControllers,
        Self::StatefulSets,
        Self::Jobs,
        Self::Services,
        Self::Ingresses,
        Self::IngressClasses,
        Self::ConfigMaps,
        Self::PersistentVolumeClaims,
        Self::Secrets,
        Self::StorageClasses,
        Self::PersistentVolumes,
        Self::ServiceAccounts,
        Self::Roles,
        Self::RoleBindings,
        Self::ClusterRoles,
        Self::ClusterRoleBindings,
        Self::NetworkPolicies,
        Self::Nodes,
        Self::Events,
        Self::Namespaces,
        Self::CustomResources,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Pods => "Pods",
            Self::CronJobs => "CronJobs",
            Self::DaemonSets => "DaemonSets",
            Self::Deployments => "Deployments",
            Self::ReplicaSets => "ReplicaSets",
            Self::ReplicationControllers => "ReplicationControllers",
            Self::StatefulSets => "StatefulSets",
            Self::Jobs => "Jobs",
            Self::Services => "Services",
            Self::Ingresses => "Ingresses",
            Self::IngressClasses => "IngressClasses",
            Self::ConfigMaps => "ConfigMaps",
            Self::PersistentVolumeClaims => "PVC",
            Self::Secrets => "Secrets",
            Self::StorageClasses => "StorageClasses",
            Self::PersistentVolumes => "PersistentVolumes",
            Self::ServiceAccounts => "ServiceAccounts",
            Self::Roles => "Roles",
            Self::RoleBindings => "RoleBindings",
            Self::ClusterRoles => "ClusterRoles",
            Self::ClusterRoleBindings => "ClusterRoleBindings",
            Self::NetworkPolicies => "NetworkPolicies",
            Self::Nodes => "Nodes",
            Self::Events => "Events",
            Self::Namespaces => "Namespaces",
            Self::CustomResources => "CRD",
        }
    }

    pub fn from_token(token: &str) -> Option<Self> {
        match token.to_ascii_lowercase().as_str() {
            "po" | "pod" | "pods" => Some(Self::Pods),
            "cj" | "cronjob" | "cronjobs" | "cron-job" | "cron-jobs" => Some(Self::CronJobs),
            "ds" | "daemonset" | "daemonsets" | "daemon-set" | "daemon-sets" => {
                Some(Self::DaemonSets)
            }
            "deploy" | "deployment" | "deployments" | "dp" => Some(Self::Deployments),
            "rs" | "replicaset" | "replicasets" | "replica-set" | "replica-sets" => {
                Some(Self::ReplicaSets)
            }
            "rc"
            | "replicationcontroller"
            | "replicationcontrollers"
            | "replication-controller"
            | "replication-controllers" => Some(Self::ReplicationControllers),
            "sts" | "statefulset" | "statefulsets" => Some(Self::StatefulSets),
            "job" | "jobs" => Some(Self::Jobs),
            "svc" | "service" | "services" => Some(Self::Services),
            "ing" | "ingress" | "ingresses" => Some(Self::Ingresses),
            "ingclass" | "ingressclass" | "ingressclasses" | "ingress-class"
            | "ingress-classes" | "ic" => Some(Self::IngressClasses),
            "cm" | "configmap" | "configmaps" | "config-map" | "config-maps" => {
                Some(Self::ConfigMaps)
            }
            "pvc"
            | "persistentvolumeclaim"
            | "persistentvolumeclaims"
            | "persistent-volume-claim"
            | "persistent-volume-claims" => Some(Self::PersistentVolumeClaims),
            "secret" | "secrets" => Some(Self::Secrets),
            "sc" | "storageclass" | "storageclasses" | "storage-class" | "storage-classes" => {
                Some(Self::StorageClasses)
            }
            "pv" | "persistentvolume" | "persistentvolumes" | "persistent-volume"
            | "persistent-volumes" => Some(Self::PersistentVolumes),
            "sa" | "serviceaccount" | "serviceaccounts" | "service-account"
            | "service-accounts" => Some(Self::ServiceAccounts),
            "role" | "roles" => Some(Self::Roles),
            "rb" | "rolebinding" | "rolebindings" | "role-binding" | "role-bindings" => {
                Some(Self::RoleBindings)
            }
            "crole" | "clusterrole" | "clusterroles" | "cluster-role" | "cluster-roles" => {
                Some(Self::ClusterRoles)
            }
            "crb"
            | "clusterrolebinding"
            | "clusterrolebindings"
            | "cluster-role-binding"
            | "cluster-role-bindings" => Some(Self::ClusterRoleBindings),
            "np" | "networkpolicy" | "networkpolicies" | "network-policy" | "network-policies" => {
                Some(Self::NetworkPolicies)
            }
            "node" | "nodes" | "no" => Some(Self::Nodes),
            "event" | "events" | "ev" => Some(Self::Events),
            "ns" | "namespace" | "namespaces" => Some(Self::Namespaces),
            "crd"
            | "crds"
            | "custom"
            | "customresources"
            | "custom-resources"
            | "customresourcedefinition"
            | "customresourcedefinitions" => Some(Self::CustomResources),
            _ => None,
        }
    }

    pub fn short_token(self) -> &'static str {
        match self {
            Self::Pods => "po",
            Self::CronJobs => "cj",
            Self::DaemonSets => "ds",
            Self::Deployments => "deploy",
            Self::ReplicaSets => "rs",
            Self::ReplicationControllers => "rc",
            Self::StatefulSets => "sts",
            Self::Jobs => "job",
            Self::Services => "svc",
            Self::Ingresses => "ing",
            Self::IngressClasses => "ingclass",
            Self::ConfigMaps => "cm",
            Self::PersistentVolumeClaims => "pvc",
            Self::Secrets => "secret",
            Self::StorageClasses => "sc",
            Self::PersistentVolumes => "pv",
            Self::ServiceAccounts => "sa",
            Self::Roles => "role",
            Self::RoleBindings => "rb",
            Self::ClusterRoles => "crole",
            Self::ClusterRoleBindings => "crb",
            Self::NetworkPolicies => "np",
            Self::Nodes => "node",
            Self::Events => "event",
            Self::Namespaces => "ns",
            Self::CustomResources => "crd",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CustomResourceDef {
    pub name: String,
    pub group: String,
    pub version: String,
    pub kind: String,
    pub plural: String,
    pub namespaced: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Default)]
pub struct PodContainerInfo {
    pub name: String,
    pub image: String,
    pub ready: bool,
    pub state: String,
    pub restarts: u32,
    pub age: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NamespaceScope {
    All,
    Named(String),
}

impl NamespaceScope {
    pub fn label(&self) -> String {
        match self {
            Self::All => "all".to_string(),
            Self::Named(namespace) => namespace.clone(),
        }
    }
}

impl Display for NamespaceScope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Named(namespace) => write!(f, "{namespace}"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RowData {
    pub name: String,
    pub namespace: Option<String>,
    pub columns: Vec<String>,
    pub detail: String,
}

impl RowData {
    pub fn matches_filter(&self, query: &str) -> bool {
        let query = query.trim();
        if query.is_empty() {
            return true;
        }

        let query_lower = query.to_ascii_lowercase();

        if self.name.to_ascii_lowercase().contains(&query_lower) {
            return true;
        }

        if let Some(namespace) = &self.namespace
            && namespace.to_ascii_lowercase().contains(&query_lower)
        {
            return true;
        }

        self.columns
            .iter()
            .any(|column| column.to_ascii_lowercase().contains(&query_lower))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TableData {
    pub headers: Vec<String>,
    pub rows: Vec<RowData>,
    pub selected: usize,
    pub last_refreshed: Option<DateTime<Local>>,
    pub error: Option<String>,
}

impl TableData {
    pub fn set_rows(
        &mut self,
        headers: Vec<String>,
        rows: Vec<RowData>,
        refreshed_at: DateTime<Local>,
    ) {
        self.headers = headers;
        self.rows = rows;
        self.last_refreshed = Some(refreshed_at);
        self.error = None;
        self.selected = self.selected.min(self.rows.len().saturating_sub(1));
    }

    pub fn set_error(&mut self, error: impl Into<String>, refreshed_at: DateTime<Local>) {
        self.rows.clear();
        self.error = Some(error.into());
        self.last_refreshed = Some(refreshed_at);
        self.selected = 0;
    }
}

#[derive(Debug, Clone, Default)]
pub struct OverviewMetrics {
    pub cpu_usage_millicores: u64,
    pub cpu_capacity_millicores: u64,
    pub memory_usage_bytes: u64,
    pub memory_capacity_bytes: u64,
    pub cpu_percent: Option<u64>,
    pub memory_percent: Option<u64>,
    pub sampled_pods: usize,
    pub sampled_nodes: usize,
    pub pod_usage: HashMap<String, (u64, u64)>,
    pub namespace_usage: HashMap<String, (u64, u64)>,
}

#[cfg(test)]
mod tests {
    use super::ResourceTab;

    #[test]
    fn resource_aliases_map_to_expected_tabs() {
        assert_eq!(ResourceTab::from_token("cj"), Some(ResourceTab::CronJobs));
        assert_eq!(
            ResourceTab::from_token("daemonsets"),
            Some(ResourceTab::DaemonSets)
        );
        assert_eq!(
            ResourceTab::from_token("rs"),
            Some(ResourceTab::ReplicaSets)
        );
        assert_eq!(
            ResourceTab::from_token("replicationcontrollers"),
            Some(ResourceTab::ReplicationControllers)
        );
        assert_eq!(ResourceTab::from_token("ing"), Some(ResourceTab::Ingresses));
        assert_eq!(
            ResourceTab::from_token("ingclass"),
            Some(ResourceTab::IngressClasses)
        );
        assert_eq!(ResourceTab::from_token("cm"), Some(ResourceTab::ConfigMaps));
        assert_eq!(
            ResourceTab::from_token("pvc"),
            Some(ResourceTab::PersistentVolumeClaims)
        );
        assert_eq!(
            ResourceTab::from_token("persistent-volume-claims"),
            Some(ResourceTab::PersistentVolumeClaims)
        );
        assert_eq!(
            ResourceTab::from_token("clusterroles"),
            Some(ResourceTab::ClusterRoles)
        );
        assert_eq!(
            ResourceTab::from_token("cluster-role-bindings"),
            Some(ResourceTab::ClusterRoleBindings)
        );
        assert_eq!(
            ResourceTab::from_token("clusterrolebindings"),
            Some(ResourceTab::ClusterRoleBindings)
        );
        assert_eq!(
            ResourceTab::from_token("np"),
            Some(ResourceTab::NetworkPolicies)
        );
        assert_eq!(
            ResourceTab::from_token("sa"),
            Some(ResourceTab::ServiceAccounts)
        );
    }
}
