use anyhow::{Context, Result};
use chrono::{Local, Utc};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Namespace, Node, PersistentVolume, PersistentVolumeClaim, Pod,
    ReplicationController, Secret, Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass, NetworkPolicy};
use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding, Role, RoleBinding};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kube::api::{DeleteParams, ListParams, LogParams, Patch, PatchParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::{Api, Client, Config, ResourceExt};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

use crate::model::{
    AlertSnapshot, ContextCatalogRow, CustomResourceDef, NamespaceScope, OverviewMetrics,
    PodContainerInfo, ResourceTab, RowData, TableData,
};

#[derive(Clone)]
pub struct KubeGateway {
    client: Client,
    context: String,
    cluster: String,
    user: String,
    default_namespace: String,
    kube_targets: Vec<KubeTarget>,
    available_clusters: Vec<String>,
    available_users: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedLogTarget {
    pub namespace: String,
    pub pod_name: String,
    pub container: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone)]
struct KubeTarget {
    context: String,
    cluster_name: String,
    cluster_server: Option<String>,
    user_name: Option<String>,
    namespace: Option<String>,
}

impl KubeGateway {
    pub async fn new() -> Result<Self> {
        Self::from_kube_selection(None, None).await
    }

    pub fn available_contexts(&self) -> Vec<String> {
        let mut contexts = self
            .kube_targets
            .iter()
            .map(|target| target.context.clone())
            .collect::<Vec<_>>();
        contexts.sort();
        contexts.dedup();
        contexts
    }

    pub fn available_clusters(&self) -> Vec<String> {
        self.available_clusters.clone()
    }

    pub fn available_users(&self) -> Vec<String> {
        self.available_users.clone()
    }

    pub fn context_catalog(&self) -> Vec<ContextCatalogRow> {
        self.kube_targets
            .iter()
            .map(|target| ContextCatalogRow {
                context: target.context.clone(),
                cluster: target.cluster_name.clone(),
                auth_info: target.user_name.clone().unwrap_or_else(|| "-".to_string()),
                namespace: target
                    .namespace
                    .clone()
                    .unwrap_or_else(|| self.default_namespace.clone()),
            })
            .collect()
    }

    pub async fn switch_context(&mut self, context: &str) -> Result<()> {
        let switched = Self::from_kube_selection(Some(context.to_string()), None).await?;
        *self = switched;
        Ok(())
    }

    pub async fn switch_cluster(&mut self, cluster: &str) -> Result<String> {
        let normalized = cluster.trim().to_ascii_lowercase();
        let Some(target_context) = self
            .kube_targets
            .iter()
            .find(|target| {
                target.cluster_name.eq_ignore_ascii_case(cluster)
                    || target
                        .cluster_server
                        .as_deref()
                        .is_some_and(|server| server.eq_ignore_ascii_case(cluster))
                    || target
                        .cluster_name
                        .to_ascii_lowercase()
                        .contains(&normalized)
                    || target
                        .cluster_server
                        .as_deref()
                        .is_some_and(|server| server.to_ascii_lowercase().contains(&normalized))
            })
            .map(|target| target.context.clone())
        else {
            anyhow::bail!("Cluster '{cluster}' was not found in kubeconfig contexts");
        };

        let switched = Self::from_kube_selection(Some(target_context.clone()), None).await?;
        *self = switched;
        Ok(target_context)
    }

    pub async fn switch_user(&mut self, user: &str) -> Result<String> {
        let normalized = user.trim().to_ascii_lowercase();
        let Some(target_context) = self
            .kube_targets
            .iter()
            .find(|target| {
                target
                    .user_name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(user))
                    || target
                        .user_name
                        .as_deref()
                        .is_some_and(|name| name.to_ascii_lowercase().contains(&normalized))
            })
            .map(|target| target.context.clone())
        else {
            anyhow::bail!("User '{user}' was not found in kubeconfig contexts");
        };

        let switched = Self::from_kube_selection(Some(target_context.clone()), None).await?;
        *self = switched;
        Ok(target_context)
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

    pub fn default_namespace(&self) -> &str {
        &self.default_namespace
    }

    pub fn client(&self) -> Client {
        self.client.clone()
    }

    async fn from_kube_selection(context: Option<String>, cluster: Option<String>) -> Result<Self> {
        let kubeconfig = Kubeconfig::read().ok();

        let config = if let Some(kubeconfig_value) = kubeconfig.clone() {
            let options = KubeConfigOptions {
                context: context.clone(),
                cluster: cluster.clone(),
                user: None,
            };
            Config::from_custom_kubeconfig(kubeconfig_value, &options)
                .await
                .context("failed to infer Kubernetes configuration")?
        } else {
            if context.is_some() || cluster.is_some() {
                anyhow::bail!(
                    "kubeconfig not found; context/cluster switching is unavailable in this environment"
                );
            }
            Config::infer()
                .await
                .context("failed to infer Kubernetes configuration")?
        };

        let cluster_url = config.cluster_url.to_string();
        let default_namespace = config.default_namespace.clone();
        let client = Client::try_from(config).context("failed to initialize Kubernetes client")?;

        let kube_targets = kubeconfig
            .as_ref()
            .map(build_kube_targets)
            .unwrap_or_default();
        let mut available_clusters = kube_targets
            .iter()
            .map(|target| target.cluster_name.clone())
            .collect::<Vec<_>>();
        for server in kube_targets
            .iter()
            .filter_map(|target| target.cluster_server.clone())
        {
            available_clusters.push(server);
        }
        available_clusters.sort();
        available_clusters.dedup();

        let mut available_users = kube_targets
            .iter()
            .filter_map(|target| target.user_name.clone())
            .collect::<Vec<_>>();
        available_users.sort();
        available_users.dedup();

        let active_context = context
            .or_else(|| {
                kubeconfig
                    .as_ref()
                    .and_then(|cfg| cfg.current_context.clone())
            })
            .unwrap_or_else(|| "in-cluster".to_string());
        let active_user = kube_targets
            .iter()
            .find(|target| target.context == active_context)
            .and_then(|target| target.user_name.clone())
            .unwrap_or_else(|| "-".to_string());

        Ok(Self {
            client,
            context: active_context,
            cluster: cluster_url,
            user: active_user,
            default_namespace,
            kube_targets,
            available_clusters,
            available_users,
        })
    }

    pub async fn fetch_table(
        &self,
        tab: ResourceTab,
        scope: &NamespaceScope,
        selected_custom: Option<&CustomResourceDef>,
    ) -> Result<TableData> {
        let refreshed_at = Local::now();
        let (headers, mut rows) = match tab {
            ResourceTab::Pods => self.fetch_pods(scope).await?,
            ResourceTab::CronJobs => self.fetch_cronjobs(scope).await?,
            ResourceTab::DaemonSets => self.fetch_daemonsets(scope).await?,
            ResourceTab::Deployments => self.fetch_deployments(scope).await?,
            ResourceTab::ReplicaSets => self.fetch_replicasets(scope).await?,
            ResourceTab::ReplicationControllers => {
                self.fetch_replication_controllers(scope).await?
            }
            ResourceTab::StatefulSets => self.fetch_statefulsets(scope).await?,
            ResourceTab::Jobs => self.fetch_jobs(scope).await?,
            ResourceTab::Services => self.fetch_services(scope).await?,
            ResourceTab::Ingresses => self.fetch_ingresses(scope).await?,
            ResourceTab::IngressClasses => self.fetch_ingress_classes().await?,
            ResourceTab::ConfigMaps => self.fetch_configmaps(scope).await?,
            ResourceTab::PersistentVolumeClaims => {
                self.fetch_persistent_volume_claims(scope).await?
            }
            ResourceTab::Secrets => self.fetch_secrets(scope).await?,
            ResourceTab::StorageClasses => self.fetch_storage_classes().await?,
            ResourceTab::PersistentVolumes => self.fetch_persistent_volumes().await?,
            ResourceTab::ServiceAccounts => self.fetch_service_accounts(scope).await?,
            ResourceTab::Roles => self.fetch_roles(scope).await?,
            ResourceTab::RoleBindings => self.fetch_role_bindings(scope).await?,
            ResourceTab::ClusterRoles => self.fetch_cluster_roles().await?,
            ResourceTab::ClusterRoleBindings => self.fetch_cluster_role_bindings().await?,
            ResourceTab::NetworkPolicies => self.fetch_network_policies(scope).await?,
            ResourceTab::Nodes => self.fetch_nodes().await?,
            ResourceTab::Events => self.fetch_events(scope).await?,
            ResourceTab::Namespaces => self.fetch_namespaces().await?,
            ResourceTab::CustomResources => {
                if let Some(custom) = selected_custom {
                    self.fetch_custom_resources(custom, scope).await?
                } else {
                    self.fetch_custom_resource_definitions().await?
                }
            }
        };

        rows.sort_by(|left, right| {
            left.namespace
                .cmp(&right.namespace)
                .then_with(|| left.name.cmp(&right.name))
        });

        let mut table = TableData::default();
        table.set_rows(headers, rows, refreshed_at);
        Ok(table)
    }

    pub async fn fetch_pod_logs(
        &self,
        namespace: &str,
        pod_name: &str,
        container: Option<&str>,
        previous: bool,
    ) -> Result<String> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let params = LogParams {
            container: container.map(str::to_string),
            previous,
            tail_lines: Some(500),
            timestamps: true,
            ..LogParams::default()
        };

        let logs = pods
            .logs(pod_name, &params)
            .await
            .with_context(|| format!("failed to load logs for {namespace}/{pod_name}"))?;

        Ok(logs)
    }

    pub async fn pod_containers(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<Vec<PodContainerInfo>> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod = pods
            .get(pod_name)
            .await
            .with_context(|| format!("failed to fetch pod {namespace}/{pod_name}"))?;

        let pod_age = human_age(pod.metadata.creation_timestamp.as_ref());
        let mut ordered = Vec::<(String, String)>::new();
        if let Some(spec) = pod.spec.as_ref() {
            for container in &spec.containers {
                ordered.push((
                    container.name.clone(),
                    container.image.clone().unwrap_or_else(|| "-".to_string()),
                ));
            }
            for container in spec.init_containers.as_ref().into_iter().flatten() {
                ordered.push((
                    container.name.clone(),
                    container.image.clone().unwrap_or_else(|| "-".to_string()),
                ));
            }
        }

        let mut statuses = HashMap::<String, PodContainerInfo>::new();
        if let Some(status) = pod.status.as_ref() {
            for container in status.container_statuses.as_ref().into_iter().flatten() {
                statuses.insert(
                    container.name.clone(),
                    pod_container_from_status(container, &pod_age),
                );
            }
            for container in status
                .init_container_statuses
                .as_ref()
                .into_iter()
                .flatten()
            {
                statuses.insert(
                    container.name.clone(),
                    pod_container_from_status(container, &pod_age),
                );
            }
        }

        let mut rows = Vec::new();
        for (name, image) in ordered {
            let mut info = statuses.remove(&name).unwrap_or_default();
            info.name = name;
            if info.image.is_empty() || info.image == "-" {
                info.image = image;
            }
            if info.age.is_empty() {
                info.age = pod_age.clone();
            }
            rows.push(info);
        }

        if rows.is_empty() {
            let mut fallback = statuses.into_values().collect::<Vec<_>>();
            fallback.sort_by(|left, right| left.name.cmp(&right.name));
            rows = fallback;
        }

        Ok(rows)
    }

    pub async fn resolve_log_target(
        &self,
        tab: ResourceTab,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<ResolvedLogTarget> {
        let namespace = namespace
            .map(str::to_string)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                if self.default_namespace.is_empty() {
                    None
                } else {
                    Some(self.default_namespace.clone())
                }
            })
            .context("namespace is required to resolve related logs")?;

        if tab == ResourceTab::Pods {
            return self
                .resolve_pod_log_target(&namespace, name)
                .await
                .map(|mut target| {
                    target.source = "pod".to_string();
                    target
                });
        }

        if tab == ResourceTab::Services {
            return self.resolve_service_log_target(&namespace, name).await;
        }

        if !matches!(
            tab,
            ResourceTab::Deployments
                | ResourceTab::DaemonSets
                | ResourceTab::StatefulSets
                | ResourceTab::ReplicaSets
                | ResourceTab::ReplicationControllers
                | ResourceTab::Jobs
                | ResourceTab::CronJobs
        ) {
            anyhow::bail!(
                "Related logs are not supported for {}. Use Pods tab or Shift+L on workloads/services",
                tab.title()
            );
        }

        self.resolve_workload_log_target(tab, &namespace, name)
            .await
    }

    pub async fn fetch_overview_metrics(&self, scope: &NamespaceScope) -> Result<OverviewMetrics> {
        let mut snapshot = OverviewMetrics::default();

        let pod_metrics_gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics");
        let pod_metrics_resource = ApiResource::from_gvk_with_plural(&pod_metrics_gvk, "pods");
        let pod_metrics_api: Api<DynamicObject> = match scope {
            NamespaceScope::All => Api::all_with(self.client.clone(), &pod_metrics_resource),
            NamespaceScope::Named(namespace) => {
                Api::namespaced_with(self.client.clone(), namespace, &pod_metrics_resource)
            }
        };

        let pod_metrics = pod_metrics_api.list(&list_params()).await?;
        snapshot.sampled_pods = pod_metrics.items.len();
        for pod_metric in pod_metrics {
            let namespace = pod_metric.namespace().unwrap_or_else(|| "-".to_string());
            let name = pod_metric.name_any();
            let (cpu_millicores, memory_bytes) = parse_pod_metrics_usage(&pod_metric.data);
            snapshot.pod_usage.insert(
                format!("{namespace}/{name}"),
                (cpu_millicores, memory_bytes),
            );

            let namespace_entry = snapshot
                .namespace_usage
                .entry(namespace)
                .or_insert((0u64, 0u64));
            namespace_entry.0 = namespace_entry.0.saturating_add(cpu_millicores);
            namespace_entry.1 = namespace_entry.1.saturating_add(memory_bytes);
        }

        let node_metrics_gvk = GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "NodeMetrics");
        let node_metrics_resource = ApiResource::from_gvk_with_plural(&node_metrics_gvk, "nodes");
        let node_metrics_api: Api<DynamicObject> =
            Api::all_with(self.client.clone(), &node_metrics_resource);
        let node_metrics = node_metrics_api.list(&list_params()).await?;
        snapshot.sampled_nodes = node_metrics.items.len();
        for node_metric in node_metrics {
            let (cpu_millicores, memory_bytes) = parse_usage_from_value(&node_metric.data["usage"]);
            snapshot.cpu_usage_millicores =
                snapshot.cpu_usage_millicores.saturating_add(cpu_millicores);
            snapshot.memory_usage_bytes = snapshot.memory_usage_bytes.saturating_add(memory_bytes);
        }

        let nodes: Api<Node> = Api::all(self.client.clone());
        let node_list = nodes.list(&list_params()).await?;
        for node in node_list {
            if let Some(allocatable) = node
                .status
                .as_ref()
                .and_then(|status| status.allocatable.as_ref())
            {
                let cpu_capacity = allocatable
                    .get("cpu")
                    .and_then(|quantity| parse_cpu_millicores(&quantity.0))
                    .unwrap_or(0);
                let memory_capacity = allocatable
                    .get("memory")
                    .and_then(|quantity| parse_memory_bytes(&quantity.0))
                    .unwrap_or(0);
                snapshot.cpu_capacity_millicores = snapshot
                    .cpu_capacity_millicores
                    .saturating_add(cpu_capacity);
                snapshot.memory_capacity_bytes = snapshot
                    .memory_capacity_bytes
                    .saturating_add(memory_capacity);
            }
        }

        if snapshot.cpu_capacity_millicores > 0 {
            snapshot.cpu_percent = Some(
                snapshot
                    .cpu_usage_millicores
                    .saturating_mul(100)
                    .saturating_div(snapshot.cpu_capacity_millicores)
                    .min(100),
            );
        }
        if snapshot.memory_capacity_bytes > 0 {
            snapshot.memory_percent = Some(
                snapshot
                    .memory_usage_bytes
                    .saturating_mul(100)
                    .saturating_div(snapshot.memory_capacity_bytes)
                    .min(100),
            );
        }

        Ok(snapshot)
    }

    pub async fn discover_custom_resources(&self) -> Result<Vec<CustomResourceDef>> {
        let crd_api: Api<CustomResourceDefinition> = Api::all(self.client.clone());
        let list = crd_api.list(&list_params()).await?;

        let mut resources = list
            .into_iter()
            .filter_map(|crd| {
                let spec = crd.spec;
                let storage_version = spec
                    .versions
                    .iter()
                    .find(|version| version.storage)
                    .or_else(|| spec.versions.first())?;

                Some(CustomResourceDef {
                    name: spec.names.plural.clone(),
                    group: spec.group.clone(),
                    version: storage_version.name.clone(),
                    kind: spec.names.kind.clone(),
                    plural: spec.names.plural,
                    namespaced: spec.scope == "Namespaced",
                })
            })
            .collect::<Vec<_>>();

        resources.sort_by(|left, right| left.name.cmp(&right.name));
        resources.dedup_by(|left, right| left.name == right.name && left.group == right.group);
        Ok(resources)
    }

    pub async fn delete_resource(
        &self,
        tab: ResourceTab,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<()> {
        let params = DeleteParams::default();
        match tab {
            ResourceTab::Pods => {
                let namespace = namespace.context("namespace is required for pod delete")?;
                let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::CronJobs => {
                let namespace = namespace.context("namespace is required for cronjob delete")?;
                let api: Api<CronJob> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::DaemonSets => {
                let namespace = namespace.context("namespace is required for daemonset delete")?;
                let api: Api<DaemonSet> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Deployments => {
                let namespace = namespace.context("namespace is required for deployment delete")?;
                let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::ReplicaSets => {
                let namespace = namespace.context("namespace is required for replicaset delete")?;
                let api: Api<ReplicaSet> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::ReplicationControllers => {
                let namespace =
                    namespace.context("namespace is required for replicationcontroller delete")?;
                let api: Api<ReplicationController> =
                    Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::StatefulSets => {
                let namespace =
                    namespace.context("namespace is required for statefulset delete")?;
                let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Jobs => {
                let namespace = namespace.context("namespace is required for job delete")?;
                let api: Api<Job> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Services => {
                let namespace = namespace.context("namespace is required for service delete")?;
                let api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Ingresses => {
                let namespace = namespace.context("namespace is required for ingress delete")?;
                let api: Api<Ingress> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::IngressClasses => {
                let api: Api<IngressClass> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::ConfigMaps => {
                let namespace = namespace.context("namespace is required for configmap delete")?;
                let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::PersistentVolumeClaims => {
                let namespace =
                    namespace.context("namespace is required for persistentvolumeclaim delete")?;
                let api: Api<PersistentVolumeClaim> =
                    Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Secrets => {
                let namespace = namespace.context("namespace is required for secret delete")?;
                let api: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::StorageClasses => {
                let api: Api<StorageClass> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::PersistentVolumes => {
                let api: Api<PersistentVolume> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::ServiceAccounts => {
                let namespace =
                    namespace.context("namespace is required for serviceaccount delete")?;
                let api: Api<ServiceAccount> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Roles => {
                let namespace = namespace.context("namespace is required for role delete")?;
                let api: Api<Role> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::RoleBindings => {
                let namespace =
                    namespace.context("namespace is required for rolebinding delete")?;
                let api: Api<RoleBinding> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::ClusterRoles => {
                let api: Api<ClusterRole> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::ClusterRoleBindings => {
                let api: Api<ClusterRoleBinding> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::NetworkPolicies => {
                let namespace =
                    namespace.context("namespace is required for networkpolicy delete")?;
                let api: Api<NetworkPolicy> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Nodes => {
                let api: Api<Node> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Namespaces => {
                let api: Api<Namespace> = Api::all(self.client.clone());
                let _ = api.delete(name, &params).await?;
            }
            ResourceTab::Events | ResourceTab::CustomResources => {
                anyhow::bail!("delete is not supported for {}", tab.title());
            }
        }

        Ok(())
    }

    pub async fn restart_workload(
        &self,
        tab: ResourceTab,
        namespace: &str,
        name: &str,
    ) -> Result<()> {
        let patch = serde_json::json!({
            "spec": {
                "template": {
                    "metadata": {
                        "annotations": {
                            "kubectl.kubernetes.io/restartedAt": Utc::now().to_rfc3339()
                        }
                    }
                }
            }
        });
        let params = PatchParams::default();

        match tab {
            ResourceTab::Deployments => {
                let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.patch(name, &params, &Patch::Merge(&patch)).await?;
            }
            ResourceTab::StatefulSets => {
                let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.patch(name, &params, &Patch::Merge(&patch)).await?;
            }
            _ => anyhow::bail!("restart is not supported for {}", tab.title()),
        }

        Ok(())
    }

    pub async fn scale_workload(
        &self,
        tab: ResourceTab,
        namespace: &str,
        name: &str,
        replicas: i32,
    ) -> Result<()> {
        let patch = serde_json::json!({ "spec": { "replicas": replicas } });
        let params = PatchParams::default();

        match tab {
            ResourceTab::Deployments => {
                let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.patch(name, &params, &Patch::Merge(&patch)).await?;
            }
            ResourceTab::StatefulSets => {
                let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
                let _ = api.patch(name, &params, &Patch::Merge(&patch)).await?;
            }
            _ => anyhow::bail!("scale is not supported for {}", tab.title()),
        }

        Ok(())
    }

    async fn fetch_pods(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let pods: Api<Pod> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = pods.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|pod| {
                let name = pod.name_any();
                let namespace = pod.namespace();
                let status = pod
                    .status
                    .as_ref()
                    .and_then(|value| value.phase.clone())
                    .unwrap_or_else(|| "Unknown".to_string());
                let node = pod
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.node_name.clone())
                    .unwrap_or_else(|| "-".to_string());
                let (ready, total, restarts) =
                    pod.status.as_ref().map(pod_readiness).unwrap_or((0, 0, 0));
                let age = human_age(pod.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        node,
                        format!("{ready}/{total}"),
                        status,
                        restarts.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&pod),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Node".to_string(),
                "Ready".to_string(),
                "Status".to_string(),
                "Restarts".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_cronjobs(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let cronjobs: Api<CronJob> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = cronjobs.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|cronjob| {
                let name = cronjob.name_any();
                let namespace = cronjob.namespace();
                let schedule = cronjob
                    .spec
                    .as_ref()
                    .map(|spec| spec.schedule.clone())
                    .unwrap_or_else(|| "-".to_string());
                let suspended = cronjob
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.suspend)
                    .unwrap_or(false);
                let active = cronjob
                    .status
                    .as_ref()
                    .and_then(|status| status.active.as_ref())
                    .map(|entries| entries.len())
                    .unwrap_or(0);
                let last = cronjob
                    .status
                    .as_ref()
                    .and_then(|status| status.last_schedule_time.as_ref())
                    .map_or_else(|| "-".to_string(), |time| human_age(Some(time)));
                let age = human_age(cronjob.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        truncate(&schedule, 28),
                        if suspended { "Yes" } else { "No" }.to_string(),
                        active.to_string(),
                        last,
                        age,
                    ],
                    detail: yaml_detail(&cronjob),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Schedule".to_string(),
                "Suspend".to_string(),
                "Active".to_string(),
                "Last".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_daemonsets(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let daemonsets: Api<DaemonSet> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = daemonsets.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|daemonset| {
                let name = daemonset.name_any();
                let namespace = daemonset.namespace();
                let desired = daemonset
                    .status
                    .as_ref()
                    .map(|status| status.desired_number_scheduled)
                    .unwrap_or(0);
                let ready = daemonset
                    .status
                    .as_ref()
                    .map(|status| status.number_ready)
                    .unwrap_or(0);
                let updated = daemonset
                    .status
                    .as_ref()
                    .and_then(|status| status.updated_number_scheduled)
                    .unwrap_or(0);
                let available = daemonset
                    .status
                    .as_ref()
                    .and_then(|status| status.number_available)
                    .unwrap_or(0);
                let age = human_age(daemonset.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        format!("{ready}/{desired}"),
                        updated.to_string(),
                        available.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&daemonset),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Ready".to_string(),
                "Updated".to_string(),
                "Available".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_deployments(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let deployments: Api<Deployment> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = deployments.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|deployment| {
                let name = deployment.name_any();
                let namespace = deployment.namespace();
                let desired = deployment
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let updated = deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.updated_replicas)
                    .unwrap_or(0);
                let available = deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.available_replicas)
                    .unwrap_or(0);
                let age = human_age(deployment.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        format!("{ready}/{desired}"),
                        updated.to_string(),
                        available.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&deployment),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Ready".to_string(),
                "Updated".to_string(),
                "Available".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_replicasets(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let replicasets: Api<ReplicaSet> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = replicasets.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|replicaset| {
                let name = replicaset.name_any();
                let namespace = replicaset.namespace();
                let desired = replicaset
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = replicaset
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let available = replicaset
                    .status
                    .as_ref()
                    .and_then(|status| status.available_replicas)
                    .unwrap_or(0);
                let age = human_age(replicaset.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        format!("{ready}/{desired}"),
                        available.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&replicaset),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Ready".to_string(),
                "Available".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_replication_controllers(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let controllers: Api<ReplicationController> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = controllers.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|controller| {
                let name = controller.name_any();
                let namespace = controller.namespace();
                let desired = controller
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let current = controller
                    .status
                    .as_ref()
                    .map(|status| status.replicas)
                    .unwrap_or(0);
                let ready = controller
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let age = human_age(controller.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        format!("{ready}/{desired}"),
                        current.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&controller),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Ready".to_string(),
                "Current".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_statefulsets(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let statefulsets: Api<StatefulSet> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = statefulsets.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|statefulset| {
                let name = statefulset.name_any();
                let namespace = statefulset.namespace();
                let desired = statefulset
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = statefulset
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let current = statefulset
                    .status
                    .as_ref()
                    .and_then(|status| status.current_replicas)
                    .unwrap_or(0);
                let age = human_age(statefulset.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        format!("{ready}/{desired}"),
                        current.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&statefulset),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Ready".to_string(),
                "Current".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_jobs(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let jobs: Api<Job> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = jobs.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|job| {
                let name = job.name_any();
                let namespace = job.namespace();
                let desired = job
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.completions)
                    .unwrap_or(1);
                let succeeded = job
                    .status
                    .as_ref()
                    .and_then(|status| status.succeeded)
                    .unwrap_or(0);
                let active = job
                    .status
                    .as_ref()
                    .and_then(|status| status.active)
                    .unwrap_or(0);
                let failed = job
                    .status
                    .as_ref()
                    .and_then(|status| status.failed)
                    .unwrap_or(0);
                let age = human_age(job.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        format!("{succeeded}/{desired}"),
                        active.to_string(),
                        failed.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&job),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Completions".to_string(),
                "Active".to_string(),
                "Failed".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_services(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let services: Api<Service> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = services.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|service| {
                let name = service.name_any();
                let namespace = service.namespace();
                let service_type = service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.type_.clone())
                    .unwrap_or_else(|| "ClusterIP".to_string());
                let cluster_ip = service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.cluster_ip.clone())
                    .unwrap_or_else(|| "-".to_string());
                let ports = service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.ports.clone())
                    .unwrap_or_default()
                    .into_iter()
                    .map(|port| {
                        let protocol = port.protocol.unwrap_or_else(|| "TCP".to_string());
                        format!("{}/{}", port.port, protocol)
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let age = human_age(service.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        service_type,
                        cluster_ip,
                        if ports.is_empty() {
                            "-".to_string()
                        } else {
                            ports
                        },
                        age,
                    ],
                    detail: yaml_detail(&service),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Type".to_string(),
                "Cluster IP".to_string(),
                "Ports".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_ingresses(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let ingresses: Api<Ingress> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = ingresses.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|ingress| {
                let name = ingress.name_any();
                let namespace = ingress.namespace();
                let class = ingress
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.ingress_class_name.clone())
                    .unwrap_or_else(|| "-".to_string());
                let hosts = ingress
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.rules.as_ref())
                    .map(|rules| {
                        rules
                            .iter()
                            .filter_map(|rule| rule.host.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let hosts = if hosts.is_empty() {
                    "-".to_string()
                } else {
                    truncate(&hosts.join(","), 28)
                };
                let address = ingress
                    .status
                    .as_ref()
                    .and_then(|status| status.load_balancer.as_ref())
                    .and_then(|lb| lb.ingress.as_ref())
                    .and_then(|entries| {
                        entries.first().map(|entry| {
                            entry
                                .ip
                                .clone()
                                .or_else(|| entry.hostname.clone())
                                .unwrap_or_else(|| "-".to_string())
                        })
                    })
                    .unwrap_or_else(|| "-".to_string());
                let tls = ingress
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.tls.as_ref())
                    .map(|items| items.len())
                    .unwrap_or(0);
                let age = human_age(ingress.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        class,
                        hosts,
                        truncate(&address, 20),
                        tls.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&ingress),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Class".to_string(),
                "Hosts".to_string(),
                "Address".to_string(),
                "TLS".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_ingress_classes(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let classes: Api<IngressClass> = Api::all(self.client.clone());
        let list = classes.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|class| {
                let name = class.name_any();
                let controller = class
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.controller.clone())
                    .unwrap_or_else(|| "-".to_string());
                let default = class
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|annotations| {
                        annotations.get("ingressclass.kubernetes.io/is-default-class")
                    })
                    .is_some_and(|value| value == "true");
                let age = human_age(class.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![
                        name,
                        truncate(&controller, 28),
                        if default { "Yes" } else { "No" }.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&class),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Controller".to_string(),
                "Default".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_configmaps(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let configmaps: Api<ConfigMap> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = configmaps.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|configmap| {
                let name = configmap.name_any();
                let namespace = configmap.namespace();
                let data = configmap
                    .data
                    .as_ref()
                    .map(|entries| entries.len())
                    .unwrap_or(0);
                let binary = configmap
                    .binary_data
                    .as_ref()
                    .map(|entries| entries.len())
                    .unwrap_or(0);
                let age = human_age(configmap.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        data.to_string(),
                        binary.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&configmap),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Data".to_string(),
                "Binary".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_persistent_volume_claims(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let pvcs: Api<PersistentVolumeClaim> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = pvcs.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|pvc| {
                let name = pvc.name_any();
                let namespace = pvc.namespace();
                let status = pvc
                    .status
                    .as_ref()
                    .and_then(|status| status.phase.clone())
                    .unwrap_or_else(|| "-".to_string());
                let volume = pvc
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.volume_name.clone())
                    .unwrap_or_else(|| "-".to_string());
                let capacity = pvc
                    .status
                    .as_ref()
                    .and_then(|status| status.capacity.as_ref())
                    .and_then(|capacity| capacity.get("storage"))
                    .map(|quantity| quantity.0.clone())
                    .unwrap_or_else(|| "-".to_string());
                let access = pvc
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.access_modes.as_ref())
                    .map(|modes| modes.join(","))
                    .filter(|modes| !modes.is_empty())
                    .unwrap_or_else(|| "-".to_string());
                let age = human_age(pvc.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        status,
                        truncate(&volume, 22),
                        capacity,
                        access,
                        age,
                    ],
                    detail: yaml_detail(&pvc),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Status".to_string(),
                "Volume".to_string(),
                "Capacity".to_string(),
                "Access".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_secrets(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let secrets: Api<Secret> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = secrets.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|secret| {
                let name = secret.name_any();
                let namespace = secret.namespace();
                let kind = secret.type_.clone().unwrap_or_else(|| "Opaque".to_string());
                let data_count = secret.data.as_ref().map(|map| map.len()).unwrap_or(0);
                let age = human_age(secret.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        truncate(&kind, 20),
                        data_count.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&secret),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Type".to_string(),
                "Data".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_storage_classes(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let classes: Api<StorageClass> = Api::all(self.client.clone());
        let list = classes.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|class| {
                let name = class.name_any();
                let provisioner = class.provisioner.clone();
                let reclaim = class
                    .reclaim_policy
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let binding = class
                    .volume_binding_mode
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let expand = class.allow_volume_expansion.unwrap_or(false);
                let default = class
                    .metadata
                    .annotations
                    .as_ref()
                    .is_some_and(|annotations| {
                        annotations
                            .get("storageclass.kubernetes.io/is-default-class")
                            .is_some_and(|value| value == "true")
                            || annotations
                                .get("storageclass.beta.kubernetes.io/is-default-class")
                                .is_some_and(|value| value == "true")
                    });
                let age = human_age(class.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![
                        name,
                        truncate(&provisioner, 22),
                        reclaim,
                        binding,
                        if expand { "Yes" } else { "No" }.to_string(),
                        if default { "Yes" } else { "No" }.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&class),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Provisioner".to_string(),
                "Reclaim".to_string(),
                "Binding".to_string(),
                "Expand".to_string(),
                "Default".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_persistent_volumes(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let pvs: Api<PersistentVolume> = Api::all(self.client.clone());
        let list = pvs.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|pv| {
                let name = pv.name_any();
                let capacity = pv
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.capacity.as_ref())
                    .and_then(|capacity| capacity.get("storage"))
                    .map(|quantity| quantity.0.clone())
                    .unwrap_or_else(|| "-".to_string());
                let access = pv
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.access_modes.as_ref())
                    .map(|modes| modes.join(","))
                    .filter(|modes| !modes.is_empty())
                    .unwrap_or_else(|| "-".to_string());
                let reclaim = pv
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.persistent_volume_reclaim_policy.clone())
                    .unwrap_or_else(|| "-".to_string());
                let status = pv
                    .status
                    .as_ref()
                    .and_then(|status| status.phase.clone())
                    .unwrap_or_else(|| "-".to_string());
                let claim = pv
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.claim_ref.as_ref())
                    .map(|claim| {
                        let namespace = claim.namespace.clone().unwrap_or_else(|| "-".to_string());
                        let name = claim.name.clone().unwrap_or_else(|| "-".to_string());
                        format!("{namespace}/{name}")
                    })
                    .unwrap_or_else(|| "-".to_string());
                let class = pv
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.storage_class_name.clone())
                    .unwrap_or_else(|| "-".to_string());
                let age = human_age(pv.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![
                        name,
                        capacity,
                        access,
                        reclaim,
                        status,
                        truncate(&claim, 26),
                        truncate(&class, 18),
                        age,
                    ],
                    detail: yaml_detail(&pv),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Capacity".to_string(),
                "Access".to_string(),
                "Reclaim".to_string(),
                "Status".to_string(),
                "Claim".to_string(),
                "Class".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_service_accounts(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let accounts: Api<ServiceAccount> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = accounts.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|account| {
                let name = account.name_any();
                let namespace = account.namespace();
                let secrets = account
                    .secrets
                    .as_ref()
                    .map(|items| items.len())
                    .unwrap_or(0);
                let age = human_age(account.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        secrets.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&account),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Secrets".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_roles(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let roles: Api<Role> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = roles.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|role| {
                let name = role.name_any();
                let namespace = role.namespace();
                let rules = role.rules.as_ref().map(|items| items.len()).unwrap_or(0);
                let age = human_age(role.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        rules.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&role),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Rules".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_role_bindings(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let role_bindings: Api<RoleBinding> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = role_bindings.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|binding| {
                let name = binding.name_any();
                let namespace = binding.namespace();
                let role = format!("{}:{}", binding.role_ref.kind, binding.role_ref.name);
                let subjects = binding
                    .subjects
                    .as_ref()
                    .map(|items| items.len())
                    .unwrap_or(0);
                let age = human_age(binding.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        truncate(&role, 26),
                        subjects.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&binding),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Role".to_string(),
                "Subjects".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_cluster_roles(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let roles: Api<ClusterRole> = Api::all(self.client.clone());
        let list = roles.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|role| {
                let name = role.name_any();
                let rules = role.rules.as_ref().map(|items| items.len()).unwrap_or(0);
                let labels = role
                    .metadata
                    .labels
                    .as_ref()
                    .map(|items| items.len())
                    .unwrap_or(0);
                let age = human_age(role.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![name, rules.to_string(), labels.to_string(), age],
                    detail: yaml_detail(&role),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Rules".to_string(),
                "Labels".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_cluster_role_bindings(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let bindings: Api<ClusterRoleBinding> = Api::all(self.client.clone());
        let list = bindings.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|binding| {
                let name = binding.name_any();
                let role = format!("{}:{}", binding.role_ref.kind, binding.role_ref.name);
                let subjects = binding
                    .subjects
                    .as_ref()
                    .map(|items| items.len())
                    .unwrap_or(0);
                let age = human_age(binding.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![name, truncate(&role, 26), subjects.to_string(), age],
                    detail: yaml_detail(&binding),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Role".to_string(),
                "Subjects".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_network_policies(
        &self,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let policies: Api<NetworkPolicy> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = policies.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|policy| {
                let name = policy.name_any();
                let namespace = policy.namespace();
                let selector = policy
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.pod_selector.as_ref())
                    .and_then(|selector| selector.match_labels.as_ref())
                    .map(|labels| {
                        if labels.is_empty() {
                            "*".to_string()
                        } else {
                            labels.len().to_string()
                        }
                    })
                    .unwrap_or_else(|| "*".to_string());
                let types = policy
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.policy_types.as_ref())
                    .map(|types| types.join(","))
                    .filter(|types| !types.is_empty())
                    .unwrap_or_else(|| "-".to_string());
                let ingress_count = policy
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.ingress.as_ref())
                    .map(|items| items.len())
                    .unwrap_or(0);
                let egress_count = policy
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.egress.as_ref())
                    .map(|items| items.len())
                    .unwrap_or(0);
                let age = human_age(policy.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        selector,
                        types,
                        format!("{ingress_count}/{egress_count}"),
                        age,
                    ],
                    detail: yaml_detail(&policy),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Selector".to_string(),
                "Types".to_string(),
                "In/Eg".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_nodes(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let list = nodes.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|node| {
                let name = node.name_any();
                let ready = node
                    .status
                    .as_ref()
                    .and_then(|status| status.conditions.as_ref())
                    .and_then(|conditions| {
                        conditions
                            .iter()
                            .find(|condition| condition.type_ == "Ready")
                    })
                    .map(|condition| condition.status.clone())
                    .map(|status| match status.as_str() {
                        "True" => "Ready".to_string(),
                        "False" => "NotReady".to_string(),
                        _ => "Unknown".to_string(),
                    })
                    .unwrap_or_else(|| "Unknown".to_string());
                let version = node
                    .status
                    .as_ref()
                    .and_then(|status| status.node_info.as_ref())
                    .map(|info| info.kubelet_version.clone())
                    .unwrap_or_else(|| "-".to_string());
                let roles = node_roles(&node);
                let age = human_age(node.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![name, ready, roles, version, age],
                    detail: yaml_detail(&node),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Ready".to_string(),
                "Roles".to_string(),
                "Version".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_events(&self, scope: &NamespaceScope) -> Result<(Vec<String>, Vec<RowData>)> {
        let events: Api<Event> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };

        let list = events.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|event| {
                let event_name = event.name_any();
                let namespace = event.namespace();
                let kind = event
                    .involved_object
                    .kind
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let object_name = event
                    .involved_object
                    .name
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let reason = event.reason.clone().unwrap_or_else(|| "-".to_string());
                let event_type = event.type_.clone().unwrap_or_else(|| "-".to_string());
                let message = event.message.clone().unwrap_or_else(|| "-".to_string());
                let age = event_age(&event);

                RowData {
                    name: event_name,
                    namespace: namespace.clone(),
                    columns: vec![
                        namespace.unwrap_or_else(|| "-".to_string()),
                        kind,
                        object_name,
                        reason,
                        event_type,
                        truncate(&message, 72),
                        age,
                    ],
                    detail: yaml_detail(&event),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Namespace".to_string(),
                "Kind".to_string(),
                "Object".to_string(),
                "Reason".to_string(),
                "Type".to_string(),
                "Message".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_namespaces(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let namespaces: Api<Namespace> = Api::all(self.client.clone());
        let list = namespaces.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|namespace| {
                let name = namespace.name_any();
                let phase = namespace
                    .status
                    .as_ref()
                    .and_then(|status| status.phase.clone())
                    .unwrap_or_else(|| "Active".to_string());
                let labels = namespace
                    .metadata
                    .labels
                    .as_ref()
                    .map(|map| map.len())
                    .unwrap_or(0);
                let age = human_age(namespace.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: Some(name.clone()),
                    columns: vec![name, phase, labels.to_string(), age],
                    detail: yaml_detail(&namespace),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Status".to_string(),
                "Labels".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_custom_resources(
        &self,
        custom: &CustomResourceDef,
        scope: &NamespaceScope,
    ) -> Result<(Vec<String>, Vec<RowData>)> {
        let gvk = GroupVersionKind::gvk(&custom.group, &custom.version, &custom.kind);
        let api_resource = ApiResource::from_gvk_with_plural(&gvk, &custom.plural);
        let resources: Api<DynamicObject> = if custom.namespaced {
            match scope {
                NamespaceScope::All => Api::all_with(self.client.clone(), &api_resource),
                NamespaceScope::Named(namespace) => {
                    Api::namespaced_with(self.client.clone(), namespace, &api_resource)
                }
            }
        } else {
            Api::all_with(self.client.clone(), &api_resource)
        };

        let list = resources.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|resource| {
                let name = resource.name_any();
                let namespace = resource.namespace();
                let age = human_age(resource.metadata.creation_timestamp.as_ref());
                let labels = resource
                    .metadata
                    .labels
                    .as_ref()
                    .map(|set| set.len())
                    .unwrap_or(0);

                RowData {
                    name: name.clone(),
                    namespace: namespace.clone(),
                    columns: vec![
                        name,
                        namespace.unwrap_or_else(|| "-".to_string()),
                        labels.to_string(),
                        age,
                    ],
                    detail: yaml_detail(&resource),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Namespace".to_string(),
                "Labels".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn fetch_custom_resource_definitions(&self) -> Result<(Vec<String>, Vec<RowData>)> {
        let crd_api: Api<CustomResourceDefinition> = Api::all(self.client.clone());
        let list = crd_api.list(&list_params()).await?;
        let rows = list
            .into_iter()
            .map(|crd| {
                let name = crd.name_any();
                let kind = crd.spec.names.kind.clone();
                let group = crd.spec.group.clone();
                let scope = crd.spec.scope.clone();
                let versions = crd
                    .spec
                    .versions
                    .iter()
                    .map(|version| version.name.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                let age = human_age(crd.metadata.creation_timestamp.as_ref());

                RowData {
                    name: name.clone(),
                    namespace: None,
                    columns: vec![name, kind, group, scope, versions, age],
                    detail: yaml_detail(&crd),
                }
            })
            .collect::<Vec<_>>();

        Ok((
            vec![
                "Name".to_string(),
                "Kind".to_string(),
                "Group".to_string(),
                "Scope".to_string(),
                "Versions".to_string(),
                "Age".to_string(),
            ],
            rows,
        ))
    }

    async fn resolve_pod_log_target(
        &self,
        namespace: &str,
        pod_name: &str,
    ) -> Result<ResolvedLogTarget> {
        let containers = self.pod_containers(namespace, pod_name).await?;
        Ok(ResolvedLogTarget {
            namespace: namespace.to_string(),
            pod_name: pod_name.to_string(),
            container: containers.first().map(|container| container.name.clone()),
            source: format!("pod {namespace}/{pod_name}"),
        })
    }

    async fn resolve_workload_log_target(
        &self,
        tab: ResourceTab,
        namespace: &str,
        name: &str,
    ) -> Result<ResolvedLogTarget> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod_list = pods
            .list(&list_params())
            .await
            .with_context(|| format!("failed to list pods in namespace '{namespace}'"))?;
        let Some(best_pod) =
            select_best_related_pod(&pod_list.items, name, owner_kind_for_tab(tab))
        else {
            anyhow::bail!(
                "No related pods were found for {} {}/{}",
                tab.title(),
                namespace,
                name
            );
        };
        let pod_name = best_pod.name_any();
        Ok(ResolvedLogTarget {
            namespace: namespace.to_string(),
            pod_name,
            container: first_pod_container(best_pod),
            source: format!("{} {namespace}/{}", tab.title(), name),
        })
    }

    async fn resolve_service_log_target(
        &self,
        namespace: &str,
        service_name: &str,
    ) -> Result<ResolvedLogTarget> {
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let service = services
            .get(service_name)
            .await
            .with_context(|| format!("failed to fetch service {namespace}/{service_name}"))?;

        let selector = service
            .spec
            .as_ref()
            .and_then(|spec| spec.selector.as_ref())
            .cloned()
            .unwrap_or_default();
        if selector.is_empty() {
            anyhow::bail!("service {namespace}/{service_name} has no selector");
        }

        let selector_query = selector
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod_list = pods
            .list(&list_params().labels(&selector_query))
            .await
            .with_context(|| {
                format!("failed to list pods for service {namespace}/{service_name}")
            })?;
        let Some(best_pod) = select_best_related_pod(&pod_list.items, service_name, None) else {
            anyhow::bail!("No pods matched selector for service {namespace}/{service_name}");
        };
        let pod_name = best_pod.name_any();
        Ok(ResolvedLogTarget {
            namespace: namespace.to_string(),
            pod_name,
            container: first_pod_container(best_pod),
            source: format!("service {namespace}/{service_name}"),
        })
    }

    pub async fn fetch_pulses_report(&self, scope: &NamespaceScope) -> Result<String> {
        let pods_api: Api<Pod> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let pods = pods_api.list(&list_params()).await?;

        let mut pod_running = 0usize;
        let mut pod_pending = 0usize;
        let mut pod_failed = 0usize;
        let mut pod_succeeded = 0usize;
        let mut pod_unknown = 0usize;
        let mut pod_not_ready = 0usize;
        let mut pod_crash_loop = 0usize;
        for pod in &pods.items {
            let phase = pod
                .status
                .as_ref()
                .and_then(|status| status.phase.as_deref())
                .unwrap_or("Unknown");
            match phase {
                "Running" => pod_running = pod_running.saturating_add(1),
                "Pending" => pod_pending = pod_pending.saturating_add(1),
                "Failed" => pod_failed = pod_failed.saturating_add(1),
                "Succeeded" => pod_succeeded = pod_succeeded.saturating_add(1),
                _ => pod_unknown = pod_unknown.saturating_add(1),
            }

            if let Some(status) = pod.status.as_ref() {
                let (ready, total, _) = pod_readiness(status);
                if total > 0 && ready < total {
                    pod_not_ready = pod_not_ready.saturating_add(1);
                }
                let is_crash_loop = status.container_statuses.as_ref().is_some_and(|statuses| {
                    statuses.iter().any(|container| {
                        container
                            .state
                            .as_ref()
                            .and_then(|state| state.waiting.as_ref())
                            .and_then(|waiting| waiting.reason.as_deref())
                            .is_some_and(|reason| reason.eq_ignore_ascii_case("CrashLoopBackOff"))
                    })
                });
                if is_crash_loop {
                    pod_crash_loop = pod_crash_loop.saturating_add(1);
                }
            }
        }

        let deployments_api: Api<Deployment> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let deployments = deployments_api.list(&list_params()).await?;
        let deployment_desired = deployments
            .items
            .iter()
            .map(|deployment| {
                deployment
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1) as i64
            })
            .sum::<i64>();
        let deployment_ready = deployments
            .items
            .iter()
            .map(|deployment| {
                deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0) as i64
            })
            .sum::<i64>();
        let deployment_available = deployments
            .items
            .iter()
            .map(|deployment| {
                deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.available_replicas)
                    .unwrap_or(0) as i64
            })
            .sum::<i64>();

        let statefulsets_api: Api<StatefulSet> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let statefulsets = statefulsets_api.list(&list_params()).await?;
        let statefulset_desired = statefulsets
            .items
            .iter()
            .map(|statefulset| {
                statefulset
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1) as i64
            })
            .sum::<i64>();
        let statefulset_ready = statefulsets
            .items
            .iter()
            .map(|statefulset| {
                statefulset
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0) as i64
            })
            .sum::<i64>();

        let daemonsets_api: Api<DaemonSet> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let daemonsets = daemonsets_api.list(&list_params()).await?;
        let daemonset_desired = daemonsets
            .items
            .iter()
            .map(|daemonset| {
                daemonset
                    .status
                    .as_ref()
                    .map(|status| status.desired_number_scheduled as i64)
                    .unwrap_or(0)
            })
            .sum::<i64>();
        let daemonset_ready = daemonsets
            .items
            .iter()
            .map(|daemonset| {
                daemonset
                    .status
                    .as_ref()
                    .map(|status| status.number_ready as i64)
                    .unwrap_or(0)
            })
            .sum::<i64>();

        let jobs_api: Api<Job> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let jobs = jobs_api.list(&list_params()).await?;
        let job_active = jobs
            .items
            .iter()
            .map(|job| {
                job.status
                    .as_ref()
                    .and_then(|status| status.active)
                    .unwrap_or(0) as i64
            })
            .sum::<i64>();
        let job_succeeded = jobs
            .items
            .iter()
            .map(|job| {
                job.status
                    .as_ref()
                    .and_then(|status| status.succeeded)
                    .unwrap_or(0) as i64
            })
            .sum::<i64>();
        let job_failed = jobs
            .items
            .iter()
            .map(|job| {
                job.status
                    .as_ref()
                    .and_then(|status| status.failed)
                    .unwrap_or(0) as i64
            })
            .sum::<i64>();

        let cronjobs_api: Api<CronJob> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let cronjobs = cronjobs_api.list(&list_params()).await?;
        let cronjob_suspended = cronjobs
            .items
            .iter()
            .filter(|cronjob| {
                cronjob
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.suspend)
                    .unwrap_or(false)
            })
            .count();

        let services_api: Api<Service> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let services = services_api.list(&list_params()).await?;
        let service_node_port = services
            .items
            .iter()
            .filter(|service| {
                service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.type_.as_deref())
                    .is_some_and(|value| value == "NodePort")
            })
            .count();
        let service_load_balancer = services
            .items
            .iter()
            .filter(|service| {
                service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.type_.as_deref())
                    .is_some_and(|value| value == "LoadBalancer")
            })
            .count();

        let nodes: Api<Node> = Api::all(self.client.clone());
        let nodes = nodes.list(&list_params()).await?;
        let node_ready = nodes
            .items
            .iter()
            .filter(|node| {
                node.status
                    .as_ref()
                    .and_then(|status| status.conditions.as_ref())
                    .and_then(|conditions| {
                        conditions
                            .iter()
                            .find(|condition| condition.type_ == "Ready")
                    })
                    .is_some_and(|condition| condition.status == "True")
            })
            .count();

        let events_api: Api<Event> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let events = events_api.list(&list_params()).await?;
        let warning_events = events
            .items
            .iter()
            .filter(|event| {
                event
                    .type_
                    .as_deref()
                    .is_some_and(|event_type| event_type.eq_ignore_ascii_case("Warning"))
            })
            .count();

        let metrics = self.fetch_overview_metrics(scope).await.ok();
        let cpu_line = if let Some(metrics) = metrics.as_ref() {
            let percent = metrics
                .cpu_percent
                .map(|value| format!("{value}%"))
                .unwrap_or_else(|| "-".to_string());
            format!(
                "󰾆 CPU {} / {} ({})",
                format_cpu_millicores(metrics.cpu_usage_millicores),
                format_cpu_millicores(metrics.cpu_capacity_millicores),
                percent
            )
        } else {
            "󰾆 CPU n/a (metrics-server unavailable or timed out)".to_string()
        };
        let memory_line = if let Some(metrics) = metrics.as_ref() {
            let percent = metrics
                .memory_percent
                .map(|value| format!("{value}%"))
                .unwrap_or_else(|| "-".to_string());
            format!(
                "󰍛 RAM {} / {} ({})",
                format_bytes(metrics.memory_usage_bytes),
                format_bytes(metrics.memory_capacity_bytes),
                percent
            )
        } else {
            "󰍛 RAM n/a (metrics-server unavailable or timed out)".to_string()
        };

        let scope_label = match scope {
            NamespaceScope::All => "all".to_string(),
            NamespaceScope::Named(namespace) => namespace.clone(),
        };
        Ok([
            format!("󰠳 Scope: {scope_label}"),
            format!(
                "󰋊 Pods total:{} run:{} pend:{} fail:{} succ:{} unk:{} notReady:{} crashLoop:{}",
                pods.items.len(),
                pod_running,
                pod_pending,
                pod_failed,
                pod_succeeded,
                pod_unknown,
                pod_not_ready,
                pod_crash_loop
            ),
            format!(
                "󰹑 Deployments:{} ready:{}/{} avail:{}",
                deployments.items.len(),
                deployment_ready,
                deployment_desired,
                deployment_available
            ),
            format!(
                "󰛨 StatefulSets:{} ready:{}/{}",
                statefulsets.items.len(),
                statefulset_ready,
                statefulset_desired
            ),
            format!(
                "󰠱 DaemonSets:{} ready:{}/{}",
                daemonsets.items.len(),
                daemonset_ready,
                daemonset_desired
            ),
            format!(
                "󰁨 Jobs:{} active:{} done:{} failed:{}  󰃰 CronJobs:{} suspended:{}",
                jobs.items.len(),
                job_active,
                job_succeeded,
                job_failed,
                cronjobs.items.len(),
                cronjob_suspended
            ),
            format!(
                "󰒓 Services:{} nodePort:{} loadBalancer:{}",
                services.items.len(),
                service_node_port,
                service_load_balancer
            ),
            format!("󰒋 Nodes ready:{}/{}", node_ready, nodes.items.len()),
            format!(
                "󰀦 Events warning:{warning_events} total:{}",
                events.items.len()
            ),
            cpu_line,
            memory_line,
            "Tip: use :xray on a selected row for relationship traces".to_string(),
        ]
        .join("\n"))
    }

    pub async fn fetch_alerts_report(&self, scope: &NamespaceScope) -> Result<String> {
        let snapshot = self.fetch_alert_snapshot(scope).await?;
        let pods_api: Api<Pod> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let pods = pods_api.list(&list_params()).await?;

        let mut crash_loop_pods = Vec::new();
        let mut pending_pods = Vec::new();
        let mut failed_pods = Vec::new();
        let mut restart_heavy_pods = Vec::new();
        for pod in &pods.items {
            let namespace = pod.namespace().unwrap_or_else(|| "-".to_string());
            let pod_name = pod.name_any();
            let phase = pod
                .status
                .as_ref()
                .and_then(|status| status.phase.clone())
                .unwrap_or_else(|| "Unknown".to_string());
            let (ready, total, restarts) =
                pod.status.as_ref().map(pod_readiness).unwrap_or((0, 0, 0));

            if phase == "Pending" {
                pending_pods.push(format!("- {namespace}/{pod_name} ready:{ready}/{total}"));
            }
            if phase == "Failed" {
                failed_pods.push(format!("- {namespace}/{pod_name} ready:{ready}/{total}"));
            }
            if restarts >= 5 {
                restart_heavy_pods.push(format!(
                    "- {namespace}/{pod_name} restarts:{restarts} phase:{phase}"
                ));
            }

            let has_crash_loop = pod.status.as_ref().is_some_and(|status| {
                status.container_statuses.as_ref().is_some_and(|statuses| {
                    statuses.iter().any(|container| {
                        container
                            .state
                            .as_ref()
                            .and_then(|state| state.waiting.as_ref())
                            .and_then(|waiting| waiting.reason.as_deref())
                            .is_some_and(|reason| reason.eq_ignore_ascii_case("CrashLoopBackOff"))
                    })
                })
            });
            if has_crash_loop {
                crash_loop_pods.push(format!(
                    "- {namespace}/{pod_name} restarts:{restarts} phase:{phase}"
                ));
            }
        }

        let nodes: Api<Node> = Api::all(self.client.clone());
        let nodes = nodes.list(&list_params()).await?;
        let mut not_ready_nodes = nodes
            .items
            .iter()
            .filter_map(|node| {
                let ready_condition = node
                    .status
                    .as_ref()
                    .and_then(|status| status.conditions.as_ref())
                    .and_then(|conditions| {
                        conditions
                            .iter()
                            .find(|condition| condition.type_ == "Ready")
                    });
                let condition = ready_condition?;
                if condition.status == "True" {
                    return None;
                }
                let reason = condition
                    .reason
                    .clone()
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| "-".to_string());
                let message = condition
                    .message
                    .clone()
                    .filter(|message| !message.is_empty())
                    .unwrap_or_else(|| "-".to_string());
                Some(format!(
                    "- {} status:{} reason:{} msg:{}",
                    node.name_any(),
                    condition.status,
                    reason,
                    truncate(&message, 88)
                ))
            })
            .collect::<Vec<_>>();
        not_ready_nodes.sort();

        let events_api: Api<Event> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let events = events_api.list(&list_params()).await?;
        let mut warning_events = events
            .items
            .into_iter()
            .filter(|event| {
                event
                    .type_
                    .as_deref()
                    .is_some_and(|event_type| event_type.eq_ignore_ascii_case("Warning"))
            })
            .collect::<Vec<_>>();
        warning_events.sort_by(|left, right| {
            event_timestamp_seconds(right).cmp(&event_timestamp_seconds(left))
        });
        let warning_lines = warning_events
            .iter()
            .take(14)
            .map(|event| {
                let namespace = event.namespace().unwrap_or_else(|| "-".to_string());
                let kind = event
                    .involved_object
                    .kind
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let object = event
                    .involved_object
                    .name
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let reason = event.reason.clone().unwrap_or_else(|| "-".to_string());
                let message = event.message.clone().unwrap_or_else(|| "-".to_string());
                format!(
                    "- [{}] {namespace} {kind}/{object} {reason} {}",
                    event_age(event),
                    truncate(&message, 86)
                )
            })
            .collect::<Vec<_>>();

        let scope_label = match scope {
            NamespaceScope::All => "all".to_string(),
            NamespaceScope::Named(namespace) => namespace.clone(),
        };

        let mut lines = vec![
            format!("󰀦 Alerts scope:{scope_label}"),
            format!(
                "summary crashloop:{} pending:{} failed:{} restarts>=5:{} warning-events:{} not-ready-nodes:{}",
                snapshot.crash_loop_pods,
                snapshot.pending_pods,
                snapshot.failed_pods,
                snapshot.restart_heavy_pods,
                snapshot.warning_events,
                snapshot.not_ready_nodes
            ),
            String::new(),
            "crashloop pods".to_string(),
        ];
        if crash_loop_pods.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(crash_loop_pods.into_iter().take(12));
        }
        lines.push(String::new());
        lines.push("pending pods".to_string());
        if pending_pods.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(pending_pods.into_iter().take(12));
        }
        lines.push(String::new());
        lines.push("failed pods".to_string());
        if failed_pods.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(failed_pods.into_iter().take(12));
        }
        lines.push(String::new());
        lines.push("restart-heavy pods".to_string());
        if restart_heavy_pods.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(restart_heavy_pods.into_iter().take(12));
        }
        lines.push(String::new());
        lines.push("not-ready nodes".to_string());
        if not_ready_nodes.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(not_ready_nodes.into_iter().take(8));
        }
        lines.push(String::new());
        lines.push("warning events".to_string());
        if warning_lines.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(warning_lines);
        }

        Ok(lines.join("\n"))
    }

    pub async fn fetch_alert_snapshot(&self, scope: &NamespaceScope) -> Result<AlertSnapshot> {
        let pods_api: Api<Pod> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let pods = pods_api.list(&list_params()).await?;

        let mut crash_loop_pods = 0usize;
        let mut pending_pods = 0usize;
        let mut failed_pods = 0usize;
        let mut restart_heavy_pods = 0usize;
        for pod in &pods.items {
            let phase = pod
                .status
                .as_ref()
                .and_then(|status| status.phase.as_deref())
                .unwrap_or("Unknown");

            if phase == "Pending" {
                pending_pods = pending_pods.saturating_add(1);
            }
            if phase == "Failed" {
                failed_pods = failed_pods.saturating_add(1);
            }

            let (_, _, restarts) = pod.status.as_ref().map(pod_readiness).unwrap_or((0, 0, 0));
            if restarts >= 5 {
                restart_heavy_pods = restart_heavy_pods.saturating_add(1);
            }

            let has_crash_loop = pod.status.as_ref().is_some_and(|status| {
                status.container_statuses.as_ref().is_some_and(|statuses| {
                    statuses.iter().any(|container| {
                        container
                            .state
                            .as_ref()
                            .and_then(|state| state.waiting.as_ref())
                            .and_then(|waiting| waiting.reason.as_deref())
                            .is_some_and(|reason| reason.eq_ignore_ascii_case("CrashLoopBackOff"))
                    })
                })
            });
            if has_crash_loop {
                crash_loop_pods = crash_loop_pods.saturating_add(1);
            }
        }

        let nodes: Api<Node> = Api::all(self.client.clone());
        let nodes = nodes.list(&list_params()).await?;
        let not_ready_nodes = nodes
            .items
            .iter()
            .filter(|node| {
                node.status
                    .as_ref()
                    .and_then(|status| status.conditions.as_ref())
                    .and_then(|conditions| {
                        conditions
                            .iter()
                            .find(|condition| condition.type_ == "Ready")
                    })
                    .is_some_and(|condition| condition.status != "True")
            })
            .count();

        let events_api: Api<Event> = match scope {
            NamespaceScope::All => Api::all(self.client.clone()),
            NamespaceScope::Named(namespace) => Api::namespaced(self.client.clone(), namespace),
        };
        let events = events_api.list(&list_params()).await?;
        let warning_events = events
            .items
            .iter()
            .filter(|event| {
                event
                    .type_
                    .as_deref()
                    .is_some_and(|event_type| event_type.eq_ignore_ascii_case("Warning"))
            })
            .count();

        Ok(AlertSnapshot {
            crash_loop_pods,
            pending_pods,
            failed_pods,
            restart_heavy_pods,
            warning_events,
            not_ready_nodes,
        })
    }

    pub async fn fetch_xray_report(
        &self,
        tab: ResourceTab,
        namespace: Option<&str>,
        name: &str,
    ) -> Result<String> {
        match tab {
            ResourceTab::Pods => {
                let namespace = resolve_namespace_target(namespace, &self.default_namespace)?;
                self.fetch_pod_xray(&namespace, name).await
            }
            ResourceTab::Deployments
            | ResourceTab::DaemonSets
            | ResourceTab::StatefulSets
            | ResourceTab::ReplicaSets
            | ResourceTab::ReplicationControllers
            | ResourceTab::Jobs
            | ResourceTab::CronJobs => {
                let namespace = resolve_namespace_target(namespace, &self.default_namespace)?;
                self.fetch_workload_xray(tab, &namespace, name).await
            }
            ResourceTab::Services => {
                let namespace = resolve_namespace_target(namespace, &self.default_namespace)?;
                self.fetch_service_xray(&namespace, name).await
            }
            ResourceTab::Nodes => self.fetch_node_xray(name).await,
            ResourceTab::Namespaces => self.fetch_namespace_xray(name).await,
            _ => anyhow::bail!("xray is not implemented for {}", tab.title()),
        }
    }

    async fn fetch_pod_xray(&self, namespace: &str, pod_name: &str) -> Result<String> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod = pods
            .get(pod_name)
            .await
            .with_context(|| format!("failed to fetch pod {namespace}/{pod_name}"))?;

        let phase = pod
            .status
            .as_ref()
            .and_then(|status| status.phase.clone())
            .unwrap_or_else(|| "Unknown".to_string());
        let node = pod
            .spec
            .as_ref()
            .and_then(|spec| spec.node_name.clone())
            .unwrap_or_else(|| "-".to_string());
        let pod_ip = pod
            .status
            .as_ref()
            .and_then(|status| status.pod_ip.clone())
            .unwrap_or_else(|| "-".to_string());
        let host_ip = pod
            .status
            .as_ref()
            .and_then(|status| status.host_ip.clone())
            .unwrap_or_else(|| "-".to_string());
        let age = human_age(pod.metadata.creation_timestamp.as_ref());
        let (ready, total, restarts) = pod.status.as_ref().map(pod_readiness).unwrap_or((0, 0, 0));
        let owner_line = pod
            .metadata
            .owner_references
            .as_ref()
            .map(|owners| {
                owners
                    .iter()
                    .map(|owner| format!("{}:{}", owner.kind, owner.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "-".to_string());

        let labels = pod.metadata.labels.clone().unwrap_or_default();
        let label_line = if labels.is_empty() {
            "-".to_string()
        } else {
            let mut pairs = labels
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>();
            pairs.sort();
            truncate(&pairs.join(", "), 180)
        };

        let containers = self
            .pod_containers(namespace, pod_name)
            .await
            .unwrap_or_default();
        let container_lines = if containers.is_empty() {
            vec!["-".to_string()]
        } else {
            containers
                .iter()
                .map(|container| {
                    format!(
                        "- {} image:{} ready:{} state:{} rst:{} age:{}",
                        container.name,
                        truncate(&container.image, 46),
                        container.ready,
                        container.state,
                        container.restarts,
                        container.age
                    )
                })
                .collect::<Vec<_>>()
        };

        let services_api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let services = services_api.list(&list_params()).await?;
        let related_services = services
            .items
            .iter()
            .filter(|service| service_selector_matches_labels(service, &labels))
            .map(|service| {
                let service_type = service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.type_.clone())
                    .unwrap_or_else(|| "ClusterIP".to_string());
                let ports = service_ports_summary(service);
                format!("- {} ({service_type}) ports:{ports}", service.name_any())
            })
            .collect::<Vec<_>>();

        let events_api: Api<Event> = Api::namespaced(self.client.clone(), namespace);
        let events = events_api.list(&list_params()).await?;
        let mut related_events = events
            .items
            .into_iter()
            .filter(|event| {
                event.involved_object.kind.as_deref() == Some("Pod")
                    && event.involved_object.name.as_deref() == Some(pod_name)
            })
            .collect::<Vec<_>>();
        related_events.sort_by(|left, right| event_age(left).cmp(&event_age(right)));
        related_events.reverse();
        let event_lines = if related_events.is_empty() {
            vec!["-".to_string()]
        } else {
            related_events
                .iter()
                .take(8)
                .map(|event| {
                    let event_type = event.type_.clone().unwrap_or_else(|| "-".to_string());
                    let reason = event.reason.clone().unwrap_or_else(|| "-".to_string());
                    let message = event.message.clone().unwrap_or_else(|| "-".to_string());
                    format!(
                        "- [{}] {} {} {}",
                        event_age(event),
                        event_type,
                        reason,
                        truncate(&message, 120)
                    )
                })
                .collect::<Vec<_>>()
        };

        let mut lines = vec![
            format!("󰋊 Pod {namespace}/{pod_name}"),
            format!("status phase:{phase} ready:{ready}/{total} restarts:{restarts} age:{age}"),
            format!("node {node} podIP:{pod_ip} hostIP:{host_ip}"),
            format!("owner {owner_line}"),
            format!("labels {label_line}"),
            String::new(),
            "containers".to_string(),
        ];
        lines.extend(container_lines);
        lines.push(String::new());
        lines.push("services".to_string());
        if related_services.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(related_services);
        }
        lines.push(String::new());
        lines.push("events".to_string());
        lines.extend(event_lines);

        Ok(lines.join("\n"))
    }

    async fn fetch_workload_xray(
        &self,
        tab: ResourceTab,
        namespace: &str,
        workload_name: &str,
    ) -> Result<String> {
        let scale_line = self
            .workload_scale_line(tab, namespace, workload_name)
            .await
            .unwrap_or_else(|_| "scale unavailable".to_string());

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod_list = pods.list(&list_params()).await?;
        let owner_kind = owner_kind_for_tab(tab);
        let mut related_pods = pod_list
            .items
            .iter()
            .map(|pod| (pod_relation_score(pod, workload_name, owner_kind), pod))
            .filter(|(score, _)| *score > 0)
            .collect::<Vec<_>>();
        related_pods.sort_by(|left, right| right.0.cmp(&left.0));

        let pod_lines = if related_pods.is_empty() {
            vec!["-".to_string()]
        } else {
            related_pods
                .iter()
                .take(14)
                .map(|(_, pod)| {
                    let phase = pod
                        .status
                        .as_ref()
                        .and_then(|status| status.phase.clone())
                        .unwrap_or_else(|| "Unknown".to_string());
                    let (ready, total, restarts) =
                        pod.status.as_ref().map(pod_readiness).unwrap_or((0, 0, 0));
                    let node = pod
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.node_name.clone())
                        .unwrap_or_else(|| "-".to_string());
                    format!(
                        "- {} phase:{} ready:{}/{} rst:{} node:{}",
                        pod.name_any(),
                        phase,
                        ready,
                        total,
                        restarts,
                        node
                    )
                })
                .collect::<Vec<_>>()
        };

        let service_label_source = related_pods
            .first()
            .and_then(|(_, pod)| pod.metadata.labels.as_ref().cloned())
            .unwrap_or_default();
        let services_api: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let services = services_api.list(&list_params()).await?;
        let related_services = services
            .items
            .iter()
            .filter(|service| service_selector_matches_labels(service, &service_label_source))
            .map(|service| {
                let service_type = service
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.type_.clone())
                    .unwrap_or_else(|| "ClusterIP".to_string());
                format!(
                    "- {} ({service_type}) ports:{}",
                    service.name_any(),
                    service_ports_summary(service)
                )
            })
            .collect::<Vec<_>>();

        let events_api: Api<Event> = Api::namespaced(self.client.clone(), namespace);
        let events = events_api.list(&list_params()).await?;
        let mut related_events = events
            .items
            .into_iter()
            .filter(|event| {
                event.involved_object.name.as_deref() == Some(workload_name)
                    && owner_kind
                        .is_none_or(|kind| event.involved_object.kind.as_deref() == Some(kind))
            })
            .collect::<Vec<_>>();
        related_events.sort_by(|left, right| event_age(left).cmp(&event_age(right)));
        related_events.reverse();
        let event_lines = if related_events.is_empty() {
            vec!["-".to_string()]
        } else {
            related_events
                .iter()
                .take(8)
                .map(|event| {
                    let event_type = event.type_.clone().unwrap_or_else(|| "-".to_string());
                    let reason = event.reason.clone().unwrap_or_else(|| "-".to_string());
                    let message = event.message.clone().unwrap_or_else(|| "-".to_string());
                    format!(
                        "- [{}] {} {} {}",
                        event_age(event),
                        event_type,
                        reason,
                        truncate(&message, 120)
                    )
                })
                .collect::<Vec<_>>()
        };

        let mut lines = vec![
            format!("󰓦 Xray {} {namespace}/{workload_name}", tab.title()),
            format!("scale {scale_line}"),
            format!("pods related:{}", related_pods.len()),
        ];
        lines.extend(pod_lines);
        lines.push(String::new());
        lines.push("services".to_string());
        if related_services.is_empty() {
            lines.push("-".to_string());
        } else {
            lines.extend(related_services);
        }
        lines.push(String::new());
        lines.push("events".to_string());
        lines.extend(event_lines);
        Ok(lines.join("\n"))
    }

    async fn fetch_service_xray(&self, namespace: &str, service_name: &str) -> Result<String> {
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let service = services
            .get(service_name)
            .await
            .with_context(|| format!("failed to fetch service {namespace}/{service_name}"))?;

        let service_type = service
            .spec
            .as_ref()
            .and_then(|spec| spec.type_.clone())
            .unwrap_or_else(|| "ClusterIP".to_string());
        let cluster_ip = service
            .spec
            .as_ref()
            .and_then(|spec| spec.cluster_ip.clone())
            .unwrap_or_else(|| "-".to_string());
        let age = human_age(service.metadata.creation_timestamp.as_ref());
        let ports = service_ports_summary(&service);
        let selector = service
            .spec
            .as_ref()
            .and_then(|spec| spec.selector.as_ref())
            .cloned()
            .unwrap_or_default();
        let selector_line = if selector.is_empty() {
            "-".to_string()
        } else {
            selector
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        };

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod_list = if selector.is_empty() {
            pods.list(&list_params()).await?
        } else {
            pods.list(&list_params().labels(&selector_query(&selector)))
                .await?
        };
        let pod_lines = if selector.is_empty() {
            vec!["- service has no selector".to_string()]
        } else if pod_list.items.is_empty() {
            vec!["- no pods matched service selector".to_string()]
        } else {
            pod_list
                .items
                .iter()
                .take(16)
                .map(|pod| {
                    let phase = pod
                        .status
                        .as_ref()
                        .and_then(|status| status.phase.clone())
                        .unwrap_or_else(|| "Unknown".to_string());
                    let (ready, total, restarts) =
                        pod.status.as_ref().map(pod_readiness).unwrap_or((0, 0, 0));
                    let node = pod
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.node_name.clone())
                        .unwrap_or_else(|| "-".to_string());
                    format!(
                        "- {} phase:{} ready:{}/{} rst:{} node:{}",
                        pod.name_any(),
                        phase,
                        ready,
                        total,
                        restarts,
                        node
                    )
                })
                .collect::<Vec<_>>()
        };

        Ok([
            format!("󰒓 Service {namespace}/{service_name}"),
            format!("type:{service_type} clusterIP:{cluster_ip} ports:{ports} age:{age}"),
            format!("selector {selector_line}"),
            String::new(),
            "pods".to_string(),
            pod_lines.join("\n"),
        ]
        .join("\n"))
    }

    async fn fetch_node_xray(&self, node_name: &str) -> Result<String> {
        let nodes: Api<Node> = Api::all(self.client.clone());
        let node = nodes
            .get(node_name)
            .await
            .with_context(|| format!("failed to fetch node {node_name}"))?;

        let ready = node
            .status
            .as_ref()
            .and_then(|status| status.conditions.as_ref())
            .and_then(|conditions| {
                conditions
                    .iter()
                    .find(|condition| condition.type_ == "Ready")
            })
            .map(|condition| condition.status.clone())
            .unwrap_or_else(|| "Unknown".to_string());
        let version = node
            .status
            .as_ref()
            .and_then(|status| status.node_info.as_ref())
            .map(|info| info.kubelet_version.clone())
            .unwrap_or_else(|| "-".to_string());
        let roles = node_roles(&node);
        let age = human_age(node.metadata.creation_timestamp.as_ref());

        let pods: Api<Pod> = Api::all(self.client.clone());
        let pod_list = pods.list(&list_params()).await?;
        let mut namespace_counts = HashMap::<String, usize>::new();
        let related_pods = pod_list
            .items
            .iter()
            .filter(|pod| {
                pod.spec
                    .as_ref()
                    .and_then(|spec| spec.node_name.as_deref())
                    .is_some_and(|name| name == node_name)
            })
            .inspect(|pod| {
                let namespace = pod.namespace().unwrap_or_else(|| "-".to_string());
                let entry = namespace_counts.entry(namespace).or_insert(0);
                *entry = entry.saturating_add(1);
            })
            .collect::<Vec<_>>();

        let pod_lines = if related_pods.is_empty() {
            vec!["-".to_string()]
        } else {
            related_pods
                .iter()
                .take(18)
                .map(|pod| {
                    let namespace = pod.namespace().unwrap_or_else(|| "-".to_string());
                    let phase = pod
                        .status
                        .as_ref()
                        .and_then(|status| status.phase.clone())
                        .unwrap_or_else(|| "Unknown".to_string());
                    format!("- {namespace}/{} phase:{phase}", pod.name_any())
                })
                .collect::<Vec<_>>()
        };

        let mut namespace_lines = namespace_counts.into_iter().collect::<Vec<_>>();
        namespace_lines.sort_by(|left, right| right.1.cmp(&left.1));
        let namespace_lines = if namespace_lines.is_empty() {
            vec!["-".to_string()]
        } else {
            namespace_lines
                .iter()
                .take(8)
                .map(|(namespace, count)| format!("- {namespace}: {count}"))
                .collect::<Vec<_>>()
        };

        let mut lines = vec![
            format!("󰒋 Node {node_name}"),
            format!("ready:{ready} roles:{roles} version:{version} age:{age}"),
            format!("pods on node:{}", related_pods.len()),
            String::new(),
            "pod namespaces".to_string(),
        ];
        lines.extend(namespace_lines);
        lines.push(String::new());
        lines.push("pods".to_string());
        lines.extend(pod_lines);
        Ok(lines.join("\n"))
    }

    async fn fetch_namespace_xray(&self, namespace: &str) -> Result<String> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let jobs: Api<Job> = Api::namespaced(self.client.clone(), namespace);
        let cronjobs: Api<CronJob> = Api::namespaced(self.client.clone(), namespace);
        let configmaps: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        let secrets: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let pvcs: Api<PersistentVolumeClaim> = Api::namespaced(self.client.clone(), namespace);
        let events: Api<Event> = Api::namespaced(self.client.clone(), namespace);

        let pods = pods.list(&list_params()).await?;
        let deployments = deployments.list(&list_params()).await?;
        let services = services.list(&list_params()).await?;
        let jobs = jobs.list(&list_params()).await?;
        let cronjobs = cronjobs.list(&list_params()).await?;
        let configmaps = configmaps.list(&list_params()).await?;
        let secrets = secrets.list(&list_params()).await?;
        let pvcs = pvcs.list(&list_params()).await?;
        let events = events.list(&list_params()).await?;

        let running_pods = pods
            .items
            .iter()
            .filter(|pod| {
                pod.status
                    .as_ref()
                    .and_then(|status| status.phase.as_deref())
                    .is_some_and(|phase| phase == "Running")
            })
            .count();
        let warning_events = events
            .items
            .iter()
            .filter(|event| {
                event
                    .type_
                    .as_deref()
                    .is_some_and(|event_type| event_type.eq_ignore_ascii_case("Warning"))
            })
            .count();

        let mut lines = vec![
            format!("󰉖 Namespace {namespace}"),
            format!("pods:{} running:{}", pods.items.len(), running_pods),
            format!(
                "deployments:{} services:{} jobs:{} cronjobs:{}",
                deployments.items.len(),
                services.items.len(),
                jobs.items.len(),
                cronjobs.items.len()
            ),
            format!(
                "configmaps:{} secrets:{} pvc:{} events:{} warnings:{}",
                configmaps.items.len(),
                secrets.items.len(),
                pvcs.items.len(),
                events.items.len(),
                warning_events
            ),
            String::new(),
            "sample workloads".to_string(),
        ];
        lines.extend(
            deployments
                .items
                .iter()
                .take(8)
                .map(|deployment| {
                    let desired = deployment
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.replicas)
                        .unwrap_or(1);
                    let ready = deployment
                        .status
                        .as_ref()
                        .and_then(|status| status.ready_replicas)
                        .unwrap_or(0);
                    format!("- {} ready:{ready}/{desired}", deployment.name_any())
                })
                .collect::<Vec<_>>(),
        );

        Ok(lines.join("\n"))
    }

    async fn workload_scale_line(
        &self,
        tab: ResourceTab,
        namespace: &str,
        name: &str,
    ) -> Result<String> {
        match tab {
            ResourceTab::Deployments => {
                let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);
                let deployment = api.get(name).await?;
                let desired = deployment
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let available = deployment
                    .status
                    .as_ref()
                    .and_then(|status| status.available_replicas)
                    .unwrap_or(0);
                Ok(format!("ready:{ready}/{desired} available:{available}"))
            }
            ResourceTab::DaemonSets => {
                let api: Api<DaemonSet> = Api::namespaced(self.client.clone(), namespace);
                let daemonset = api.get(name).await?;
                let desired = daemonset
                    .status
                    .as_ref()
                    .map(|status| status.desired_number_scheduled)
                    .unwrap_or(0);
                let ready = daemonset
                    .status
                    .as_ref()
                    .map(|status| status.number_ready)
                    .unwrap_or(0);
                let available = daemonset
                    .status
                    .as_ref()
                    .and_then(|status| status.number_available)
                    .unwrap_or(0);
                Ok(format!("ready:{ready}/{desired} available:{available}"))
            }
            ResourceTab::StatefulSets => {
                let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);
                let statefulset = api.get(name).await?;
                let desired = statefulset
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = statefulset
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let current = statefulset
                    .status
                    .as_ref()
                    .and_then(|status| status.current_replicas)
                    .unwrap_or(0);
                Ok(format!("ready:{ready}/{desired} current:{current}"))
            }
            ResourceTab::ReplicaSets => {
                let api: Api<ReplicaSet> = Api::namespaced(self.client.clone(), namespace);
                let replicaset = api.get(name).await?;
                let desired = replicaset
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = replicaset
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let available = replicaset
                    .status
                    .as_ref()
                    .and_then(|status| status.available_replicas)
                    .unwrap_or(0);
                Ok(format!("ready:{ready}/{desired} available:{available}"))
            }
            ResourceTab::ReplicationControllers => {
                let api: Api<ReplicationController> =
                    Api::namespaced(self.client.clone(), namespace);
                let controller = api.get(name).await?;
                let desired = controller
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.replicas)
                    .unwrap_or(1);
                let ready = controller
                    .status
                    .as_ref()
                    .and_then(|status| status.ready_replicas)
                    .unwrap_or(0);
                let current = controller
                    .status
                    .as_ref()
                    .map(|status| status.replicas)
                    .unwrap_or(0);
                Ok(format!("ready:{ready}/{desired} current:{current}"))
            }
            ResourceTab::Jobs => {
                let api: Api<Job> = Api::namespaced(self.client.clone(), namespace);
                let job = api.get(name).await?;
                let completions = job
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.completions)
                    .unwrap_or(1);
                let active = job
                    .status
                    .as_ref()
                    .and_then(|status| status.active)
                    .unwrap_or(0);
                let succeeded = job
                    .status
                    .as_ref()
                    .and_then(|status| status.succeeded)
                    .unwrap_or(0);
                let failed = job
                    .status
                    .as_ref()
                    .and_then(|status| status.failed)
                    .unwrap_or(0);
                Ok(format!(
                    "active:{active} done:{succeeded}/{completions} failed:{failed}"
                ))
            }
            ResourceTab::CronJobs => {
                let api: Api<CronJob> = Api::namespaced(self.client.clone(), namespace);
                let cronjob = api.get(name).await?;
                let schedule = cronjob
                    .spec
                    .as_ref()
                    .map(|spec| spec.schedule.clone())
                    .unwrap_or_else(|| "-".to_string());
                let suspended = cronjob
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.suspend)
                    .unwrap_or(false);
                let active = cronjob
                    .status
                    .as_ref()
                    .and_then(|status| status.active.as_ref())
                    .map(|items| items.len())
                    .unwrap_or(0);
                Ok(format!(
                    "schedule:{} suspend:{} active:{}",
                    truncate(&schedule, 36),
                    suspended,
                    active
                ))
            }
            _ => Ok("n/a".to_string()),
        }
    }
}

fn build_kube_targets(kubeconfig: &Kubeconfig) -> Vec<KubeTarget> {
    let mut cluster_servers = HashMap::new();
    for cluster in &kubeconfig.clusters {
        let server = cluster
            .cluster
            .as_ref()
            .and_then(|entry| entry.server.clone());
        cluster_servers.insert(cluster.name.clone(), server);
    }

    let mut targets = kubeconfig
        .contexts
        .iter()
        .filter_map(|named| {
            let context = named.context.as_ref()?;
            Some(KubeTarget {
                context: named.name.clone(),
                cluster_name: context.cluster.clone(),
                cluster_server: cluster_servers
                    .get(&context.cluster)
                    .cloned()
                    .unwrap_or(None),
                user_name: context.user.clone(),
                namespace: context.namespace.clone(),
            })
        })
        .collect::<Vec<_>>();

    targets.sort_by(|left, right| {
        left.context
            .cmp(&right.context)
            .then_with(|| left.cluster_name.cmp(&right.cluster_name))
    });
    targets
}

fn owner_kind_for_tab(tab: ResourceTab) -> Option<&'static str> {
    match tab {
        ResourceTab::Deployments => Some("Deployment"),
        ResourceTab::DaemonSets => Some("DaemonSet"),
        ResourceTab::StatefulSets => Some("StatefulSet"),
        ResourceTab::ReplicaSets => Some("ReplicaSet"),
        ResourceTab::ReplicationControllers => Some("ReplicationController"),
        ResourceTab::Jobs => Some("Job"),
        ResourceTab::CronJobs => Some("CronJob"),
        _ => None,
    }
}

fn select_best_related_pod<'a>(
    pods: &'a [Pod],
    resource_name: &str,
    expected_owner_kind: Option<&str>,
) -> Option<&'a Pod> {
    pods.iter()
        .max_by_key(|pod| pod_relation_score(pod, resource_name, expected_owner_kind))
        .filter(|pod| pod_relation_score(pod, resource_name, expected_owner_kind) > 0)
}

fn pod_relation_score(pod: &Pod, resource_name: &str, expected_owner_kind: Option<&str>) -> u64 {
    let resource = resource_name.to_ascii_lowercase();
    let pod_name = pod.name_any();
    let pod_name_lower = pod_name.to_ascii_lowercase();
    let mut score = 0u64;

    if pod_name_lower == resource {
        score = score.saturating_add(600);
    }
    if pod_name_lower.starts_with(&format!("{resource}-")) {
        score = score.saturating_add(420);
    }
    if pod_name_lower.contains(&resource) {
        score = score.saturating_add(160);
    }

    if let Some(owner_refs) = pod.metadata.owner_references.as_ref() {
        for owner in owner_refs {
            let owner_name = owner.name.to_ascii_lowercase();
            if owner_name == resource {
                score = score.saturating_add(560);
            }
            if owner_name.starts_with(&format!("{resource}-")) {
                score = score.saturating_add(360);
            }
            if owner_name.contains(&resource) {
                score = score.saturating_add(140);
            }
            if let Some(kind) = expected_owner_kind
                && owner.kind.eq_ignore_ascii_case(kind)
            {
                score = score.saturating_add(220);
                if owner_name == resource {
                    score = score.saturating_add(280);
                }
            }
        }
    }

    if score > 0 || expected_owner_kind.is_none() {
        score.saturating_add(pod_running_score(pod))
    } else {
        score
    }
}

fn pod_running_score(pod: &Pod) -> u64 {
    pod.status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .map(|phase| {
            if phase.eq_ignore_ascii_case("Running") {
                48
            } else if phase.eq_ignore_ascii_case("Pending") {
                18
            } else {
                6
            }
        })
        .unwrap_or(0)
}

fn first_pod_container(pod: &Pod) -> Option<String> {
    pod.spec
        .as_ref()
        .and_then(|spec| spec.containers.first())
        .map(|container| container.name.clone())
}

fn pod_container_from_status(
    container: &k8s_openapi::api::core::v1::ContainerStatus,
    pod_age: &str,
) -> PodContainerInfo {
    let (state, age) = container_state_and_age(container, pod_age);
    PodContainerInfo {
        name: container.name.clone(),
        image: container.image.clone(),
        ready: container.ready,
        state,
        restarts: container.restart_count as u32,
        age,
    }
}

fn container_state_and_age(
    container: &k8s_openapi::api::core::v1::ContainerStatus,
    pod_age: &str,
) -> (String, String) {
    if let Some(state) = container.state.as_ref() {
        if let Some(running) = state.running.as_ref() {
            let age = running
                .started_at
                .as_ref()
                .map(|time| human_age(Some(time)))
                .unwrap_or_else(|| pod_age.to_string());
            return ("Running".to_string(), age);
        }
        if let Some(waiting) = state.waiting.as_ref() {
            let label = waiting
                .reason
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| "Waiting".to_string());
            return (label, pod_age.to_string());
        }
        if let Some(terminated) = state.terminated.as_ref() {
            let label = terminated
                .reason
                .clone()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("Exit({})", terminated.exit_code));
            let age = terminated
                .finished_at
                .as_ref()
                .map(|time| human_age(Some(time)))
                .unwrap_or_else(|| pod_age.to_string());
            return (label, age);
        }
    }

    if let Some(last_state) = container.last_state.as_ref()
        && let Some(terminated) = last_state.terminated.as_ref()
    {
        let label = terminated
            .reason
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| format!("Exit({})", terminated.exit_code));
        let age = terminated
            .finished_at
            .as_ref()
            .map(|time| human_age(Some(time)))
            .unwrap_or_else(|| pod_age.to_string());
        return (label, age);
    }

    ("Unknown".to_string(), pod_age.to_string())
}

fn parse_pod_metrics_usage(data: &Value) -> (u64, u64) {
    let Some(containers) = data.get("containers").and_then(Value::as_array) else {
        return (0, 0);
    };

    containers
        .iter()
        .fold((0u64, 0u64), |(cpu, memory), container| {
            let (container_cpu, container_memory) = container
                .get("usage")
                .map(parse_usage_from_value)
                .unwrap_or((0, 0));
            (
                cpu.saturating_add(container_cpu),
                memory.saturating_add(container_memory),
            )
        })
}

fn parse_usage_from_value(value: &Value) -> (u64, u64) {
    let cpu = value
        .get("cpu")
        .and_then(Value::as_str)
        .and_then(parse_cpu_millicores)
        .unwrap_or(0);
    let memory = value
        .get("memory")
        .and_then(Value::as_str)
        .and_then(parse_memory_bytes)
        .unwrap_or(0);
    (cpu, memory)
}

fn parse_cpu_millicores(value: &str) -> Option<u64> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }

    let (number, multiplier) = if let Some(number) = raw.strip_suffix('m') {
        (number, 1.0)
    } else if let Some(number) = raw.strip_suffix('u') {
        (number, 0.001)
    } else if let Some(number) = raw.strip_suffix('n') {
        (number, 0.000001)
    } else {
        (raw, 1000.0)
    };

    let numeric = number.parse::<f64>().ok()?;
    let millicores = (numeric * multiplier).round();
    if !millicores.is_finite() || millicores < 0.0 {
        return None;
    }
    Some(millicores as u64)
}

fn parse_memory_bytes(value: &str) -> Option<u64> {
    const BINARY_UNITS: [(&str, f64); 6] = [
        ("Ei", 1_152_921_504_606_846_976.0),
        ("Pi", 1_125_899_906_842_624.0),
        ("Ti", 1_099_511_627_776.0),
        ("Gi", 1_073_741_824.0),
        ("Mi", 1_048_576.0),
        ("Ki", 1_024.0),
    ];
    const DECIMAL_UNITS: [(&str, f64); 6] = [
        ("E", 1_000_000_000_000_000_000.0),
        ("P", 1_000_000_000_000_000.0),
        ("T", 1_000_000_000_000.0),
        ("G", 1_000_000_000.0),
        ("M", 1_000_000.0),
        ("K", 1_000.0),
    ];

    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }

    for (suffix, multiplier) in BINARY_UNITS {
        if let Some(number) = raw.strip_suffix(suffix) {
            let numeric = number.parse::<f64>().ok()?;
            let bytes = (numeric * multiplier).round();
            if !bytes.is_finite() || bytes < 0.0 {
                return None;
            }
            return Some(bytes as u64);
        }
    }

    for (suffix, multiplier) in DECIMAL_UNITS {
        if let Some(number) = raw.strip_suffix(suffix) {
            let numeric = number.parse::<f64>().ok()?;
            let bytes = (numeric * multiplier).round();
            if !bytes.is_finite() || bytes < 0.0 {
                return None;
            }
            return Some(bytes as u64);
        }
    }

    if let Some(number) = raw.strip_suffix('m') {
        let numeric = number.parse::<f64>().ok()?;
        let bytes = (numeric * 0.001).round();
        if !bytes.is_finite() || bytes < 0.0 {
            return None;
        }
        return Some(bytes as u64);
    }

    let bytes = raw.parse::<f64>().ok()?;
    if !bytes.is_finite() || bytes < 0.0 {
        return None;
    }
    Some(bytes.round() as u64)
}

fn list_params() -> ListParams {
    ListParams::default().limit(500)
}

fn resolve_namespace_target(namespace: Option<&str>, fallback: &str) -> Result<String> {
    let namespace = namespace
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            if fallback.trim().is_empty() {
                None
            } else {
                Some(fallback.to_string())
            }
        });
    namespace.context("namespace target is required")
}

fn selector_query(selector: &BTreeMap<String, String>) -> String {
    selector
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn service_selector_matches_labels(service: &Service, labels: &BTreeMap<String, String>) -> bool {
    let Some(selector) = service
        .spec
        .as_ref()
        .and_then(|spec| spec.selector.as_ref())
    else {
        return false;
    };
    if selector.is_empty() || labels.is_empty() {
        return false;
    }
    selector
        .iter()
        .all(|(key, value)| labels.get(key) == Some(value))
}

fn service_ports_summary(service: &Service) -> String {
    let ports = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.clone())
        .unwrap_or_default();
    if ports.is_empty() {
        return "-".to_string();
    }

    ports
        .into_iter()
        .map(|port| {
            let protocol = port.protocol.unwrap_or_else(|| "TCP".to_string());
            format!("{}/{}", port.port, protocol)
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn format_cpu_millicores(value: u64) -> String {
    if value >= 1_000 {
        let cores = value as f64 / 1_000.0;
        format!("{cores:.2}c")
    } else {
        format!("{value}m")
    }
}

fn format_bytes(value: u64) -> String {
    const UNITS: [(&str, f64); 6] = [
        ("Ei", 1_152_921_504_606_846_976.0),
        ("Pi", 1_125_899_906_842_624.0),
        ("Ti", 1_099_511_627_776.0),
        ("Gi", 1_073_741_824.0),
        ("Mi", 1_048_576.0),
        ("Ki", 1_024.0),
    ];
    if value == 0 {
        return "0B".to_string();
    }

    let value_f64 = value as f64;
    for (suffix, unit_size) in UNITS {
        if value_f64 >= unit_size {
            return format!("{:.1}{suffix}", value_f64 / unit_size);
        }
    }
    format!("{value}B")
}

fn pod_readiness(status: &k8s_openapi::api::core::v1::PodStatus) -> (usize, usize, i32) {
    let container_statuses = status.container_statuses.as_deref().unwrap_or(&[]);
    let total = container_statuses.len();
    let ready = container_statuses
        .iter()
        .filter(|container| container.ready)
        .count();
    let restarts = container_statuses
        .iter()
        .map(|container| container.restart_count)
        .sum();

    (ready, total, restarts)
}

fn node_roles(node: &Node) -> String {
    let Some(labels) = node.metadata.labels.as_ref() else {
        return "-".to_string();
    };

    let mut roles = labels
        .keys()
        .filter_map(|key| key.strip_prefix("node-role.kubernetes.io/"))
        .map(|role| {
            if role.is_empty() {
                "worker".to_string()
            } else {
                role.to_string()
            }
        })
        .collect::<Vec<_>>();

    if roles.is_empty()
        && labels.contains_key("kubernetes.io/role")
        && let Some(role) = labels.get("kubernetes.io/role")
    {
        roles.push(role.clone());
    }

    if roles.is_empty() {
        "-".to_string()
    } else {
        roles.sort();
        roles.dedup();
        roles.join(",")
    }
}

fn event_age(event: &Event) -> String {
    if let Some(event_time) = event.event_time.as_ref() {
        return human_age_timestamp(event_time.0);
    }

    if let Some(last_timestamp) = event.last_timestamp.as_ref() {
        return human_age(Some(last_timestamp));
    }

    if let Some(first_timestamp) = event.first_timestamp.as_ref() {
        return human_age(Some(first_timestamp));
    }

    human_age(event.metadata.creation_timestamp.as_ref())
}

fn event_timestamp_seconds(event: &Event) -> i64 {
    event
        .event_time
        .as_ref()
        .map(|time| time.0.as_second())
        .or_else(|| event.last_timestamp.as_ref().map(|time| time.0.as_second()))
        .or_else(|| {
            event
                .first_timestamp
                .as_ref()
                .map(|time| time.0.as_second())
        })
        .or_else(|| {
            event
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|time| time.0.as_second())
        })
        .unwrap_or(0)
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }

    let mut out = value
        .chars()
        .take(max.saturating_sub(1))
        .collect::<String>();
    out.push('…');
    out
}

fn human_age(timestamp: Option<&Time>) -> String {
    let Some(timestamp) = timestamp else {
        return "-".to_string();
    };

    human_age_timestamp(timestamp.0)
}

fn human_age_timestamp(ts: k8s_openapi::jiff::Timestamp) -> String {
    let elapsed_seconds = (k8s_openapi::jiff::Timestamp::now().as_second() - ts.as_second()).max(0);
    format_elapsed_seconds(elapsed_seconds)
}

fn format_elapsed_seconds(seconds: i64) -> String {
    if seconds >= 86_400 {
        return format!("{}d", seconds / 86_400);
    }

    if seconds >= 3_600 {
        return format!("{}h", seconds / 3_600);
    }

    if seconds >= 60 {
        return format!("{}m", seconds / 60);
    }

    format!("{seconds}s")
}

fn yaml_detail<T>(value: &T) -> String
where
    T: Serialize,
{
    serde_yaml::to_string(value).unwrap_or_else(|error| format!("failed to format detail: {error}"))
}
