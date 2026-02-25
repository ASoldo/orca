use crate::app::{HotkeyCommandDef, PluginCommandDef};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct RuntimeConfigSnapshot {
    pub source: Option<String>,
    pub aliases: HashMap<String, String>,
    pub plugins: Vec<PluginCommandDef>,
    pub hotkeys: Vec<HotkeyCommandDef>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfigWatcher {
    path: Option<PathBuf>,
    modified: Option<SystemTime>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct OrcaConfigFile {
    #[serde(default)]
    aliases: BTreeMap<String, String>,
    #[serde(default)]
    plugins: Vec<PluginSpec>,
    #[serde(default)]
    hotkeys: Vec<HotkeySpec>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct PluginSpec {
    name: String,
    #[serde(default, alias = "cmd", alias = "run")]
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    description: String,
    #[serde(default)]
    mutating: bool,
    #[serde(
        default = "default_plugin_timeout_secs",
        alias = "timeout",
        alias = "timeout_s"
    )]
    timeout_secs: u64,
    #[serde(default)]
    retries: u8,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct HotkeySpec {
    key: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    jump: bool,
}

impl RuntimeConfigWatcher {
    pub fn discover() -> Self {
        Self {
            path: discover_config_path(),
            modified: None,
        }
    }

    pub fn load_current(&mut self) -> Result<RuntimeConfigSnapshot> {
        let Some(path) = self.path.clone() else {
            return Ok(RuntimeConfigSnapshot {
                source: None,
                aliases: HashMap::new(),
                plugins: Vec::new(),
                hotkeys: Vec::new(),
            });
        };

        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read runtime config {}", path.display()))?;
        let parsed: OrcaConfigFile = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse runtime config {}", path.display()))?;
        self.modified = fs::metadata(&path)
            .ok()
            .and_then(|meta| meta.modified().ok());

        let aliases = parsed.aliases.into_iter().collect::<HashMap<_, _>>();
        let plugins = parsed
            .plugins
            .into_iter()
            .map(|plugin| PluginCommandDef {
                name: plugin.name,
                command: plugin.command,
                args: plugin.args,
                description: plugin.description,
                mutating: plugin.mutating,
                timeout_secs: plugin.timeout_secs,
                retries: plugin.retries,
            })
            .collect::<Vec<_>>();
        let hotkeys = parsed
            .hotkeys
            .into_iter()
            .map(|hotkey| HotkeyCommandDef {
                key: hotkey.key,
                command: hotkey.command,
                jump: hotkey.jump,
                description: hotkey.description,
            })
            .collect::<Vec<_>>();

        Ok(RuntimeConfigSnapshot {
            source: Some(path.display().to_string()),
            aliases,
            plugins,
            hotkeys,
        })
    }

    pub fn reload_if_changed(&mut self) -> Result<Option<RuntimeConfigSnapshot>> {
        if self.path.is_none() {
            self.path = discover_config_path();
            if self.path.is_some() {
                return self.load_current().map(Some);
            }
            return Ok(None);
        }

        let current_path = self.path.clone().unwrap_or_default();
        if !current_path.exists() {
            self.path = discover_config_path();
            self.modified = None;
            if self.path.is_some() {
                return self.load_current().map(Some);
            }
            return Ok(Some(RuntimeConfigSnapshot {
                source: None,
                aliases: HashMap::new(),
                plugins: Vec::new(),
                hotkeys: Vec::new(),
            }));
        }

        let modified = fs::metadata(&current_path)
            .ok()
            .and_then(|meta| meta.modified().ok());
        if modified != self.modified {
            return self.load_current().map(Some);
        }

        Ok(None)
    }
}

fn default_plugin_timeout_secs() -> u64 {
    20
}

fn discover_config_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("ORCA_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(PathBuf::from(path));
    }

    let cwd_candidates = [
        PathBuf::from("orca.yaml"),
        PathBuf::from("orca.yml"),
        PathBuf::from(".orca.yaml"),
    ];
    for candidate in cwd_candidates {
        if candidate.exists() {
            return Some(candidate);
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let user_candidates = [
            PathBuf::from(&home).join(".config/orca/config.yaml"),
            PathBuf::from(&home).join(".config/orca/config.yml"),
            PathBuf::from(&home).join(".orca.yaml"),
        ];
        for candidate in user_candidates {
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    None
}
