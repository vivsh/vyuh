use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::Serialize;
use sysinfo::{Pid, System};

use crate::Site;

#[derive(Clone)]
pub(crate) struct ConsoleStatusCache(Arc<Mutex<Option<StatusCache>>>);

#[derive(Clone)]
struct StatusCache {
    refreshed_at: Instant,
    status: StatusOut,
}

impl ConsoleStatusCache {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    pub(crate) fn get(&self, site: &Site, ttl: Duration) -> StatusOut {
        let now = Instant::now();
        if let Some(status) = self.cached(now, ttl) {
            return status;
        }
        let status = collect(site);
        *self.0.lock() = Some(StatusCache {
            refreshed_at: now,
            status: status.clone(),
        });
        status
    }

    fn cached(&self, now: Instant, ttl: Duration) -> Option<StatusOut> {
        self.0
            .lock()
            .as_ref()
            .filter(|cache| now.duration_since(cache.refreshed_at) < ttl)
            .map(|cache| cache.status.clone())
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct StatusOut {
    pub site: SiteStatus,
    pub process: ProcessStatus,
    pub system: SystemStatus,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SiteStatus {
    pub vyuh_version: &'static str,
    pub package_name: &'static str,
    pub host: String,
    pub port: u16,
    pub project_dir: String,
    pub timezone: String,
    pub database_backend: &'static str,
    pub uptime_seconds: u64,
    pub features: Vec<&'static str>,
    pub operation_count: usize,
    pub command_count: usize,
    pub service_count: usize,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ProcessStatus {
    pub pid: u32,
    pub executable_path: Option<String>,
    pub current_dir: Option<String>,
    pub argv: Vec<String>,
    pub memory_bytes: Option<u64>,
    pub virtual_memory_bytes: Option<u64>,
    pub cpu_percent: Option<f32>,
    pub thread_count: Option<usize>,
    pub open_file_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SystemStatus {
    pub hostname: Option<String>,
    pub os_name: Option<String>,
    pub os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub architecture: &'static str,
    pub cpu_brand: Option<String>,
    pub cpu_count: usize,
    pub global_cpu_percent: f32,
    pub load_average: LoadAverage,
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub total_swap_bytes: u64,
    pub used_swap_bytes: u64,
    pub boot_time_seconds: u64,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct LoadAverage {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

pub fn collect(site: &Site) -> StatusOut {
    let mut system = System::new_all();
    system.refresh_all();
    let pid = std::process::id();
    let process = system.process(Pid::from_u32(pid));
    let conf = site.conf();
    let load = System::load_average();

    StatusOut {
        site: SiteStatus {
            vyuh_version: env!("CARGO_PKG_VERSION"),
            package_name: env!("CARGO_PKG_NAME"),
            host: conf.host.clone(),
            port: conf.port,
            project_dir: site.project_dir().display().to_string(),
            timezone: site.timezone().to_string(),
            database_backend: database_backend(),
            uptime_seconds: site.uptime().as_secs(),
            features: enabled_features(),
            operation_count: site.operations().list().count(),
            command_count: site.console_command_infos().len(),
            service_count: site.console_service_infos().len(),
        },
        process: ProcessStatus {
            pid,
            executable_path: process
                .and_then(|process| process.exe())
                .map(|path| path.display().to_string()),
            current_dir: std::env::current_dir()
                .ok()
                .map(|path| path.display().to_string()),
            argv: std::env::args().collect(),
            memory_bytes: process.map(|process| process.memory()),
            virtual_memory_bytes: process.map(|process| process.virtual_memory()),
            cpu_percent: process.map(|process| process.cpu_usage()),
            thread_count: None,
            open_file_count: None,
        },
        system: SystemStatus {
            hostname: System::host_name(),
            os_name: System::name(),
            os_version: System::os_version(),
            kernel_version: System::kernel_version(),
            architecture: std::env::consts::ARCH,
            cpu_brand: system.cpus().first().map(|cpu| cpu.brand().to_string()),
            cpu_count: system.cpus().len(),
            global_cpu_percent: system.global_cpu_usage(),
            load_average: LoadAverage {
                one: load.one,
                five: load.five,
                fifteen: load.fifteen,
            },
            total_memory_bytes: system.total_memory(),
            used_memory_bytes: system.used_memory(),
            available_memory_bytes: system.available_memory(),
            total_swap_bytes: system.total_swap(),
            used_swap_bytes: system.used_swap(),
            boot_time_seconds: System::boot_time(),
        },
    }
}

fn enabled_features() -> Vec<&'static str> {
    let mut features = Vec::new();
    if cfg!(feature = "postgres") {
        features.push("postgres");
    }
    if cfg!(feature = "mysql") {
        features.push("mysql");
    }
    if cfg!(feature = "sqlite") {
        features.push("sqlite");
    }
    if cfg!(feature = "cors") {
        features.push("cors");
    }
    if cfg!(feature = "argon2") {
        features.push("argon2");
    }
    features
}

fn database_backend() -> &'static str {
    if cfg!(feature = "postgres") {
        "postgres"
    } else if cfg!(feature = "mysql") {
        "mysql"
    } else if cfg!(feature = "sqlite") {
        "sqlite"
    } else {
        "unknown"
    }
}
