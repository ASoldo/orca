use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "orca",
    version,
    about = "A modern Kubernetes terminal cockpit for DevOps teams."
)]
pub struct CliArgs {
    /// Refresh interval in milliseconds
    #[arg(long, default_value_t = 1_500)]
    pub refresh_ms: u64,

    /// Start in a specific namespace
    #[arg(short, long)]
    pub namespace: Option<String>,

    /// Start with all namespaces selected
    #[arg(short = 'A', long)]
    pub all_namespaces: bool,

    /// tracing filter (for example: info,debug,trace)
    #[arg(long, default_value = "info")]
    pub log_filter: String,
}
