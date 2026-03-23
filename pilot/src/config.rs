use std::env;

#[derive(Clone)]
pub struct Config {
    pub data_dir: String,
    pub http_port: u16,
    pub grpc_port: u16,
    pub namespace: String,
    pub database_url: String,
    pub rocksdb_path: String,
    /// Basic auth password for write operations. None = auth disabled.
    pub auth_password: Option<String>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("data_dir", &self.data_dir)
            .field("http_port", &self.http_port)
            .field("grpc_port", &self.grpc_port)
            .field("namespace", &self.namespace)
            .field("database_url", &self.database_url)
            .field("rocksdb_path", &self.rocksdb_path)
            .field("auth_password", &self.auth_password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Self {
        let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "/data".to_string());
        Self {
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| format!("sqlite://{data_dir}/regret.db")),
            rocksdb_path: env::var("ROCKSDB_PATH")
                .unwrap_or_else(|_| format!("{data_dir}/rocksdb")),
            data_dir,
            http_port: env::var("HTTP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8080),
            grpc_port: env::var("GRPC_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(9090),
            namespace: env::var("NAMESPACE").unwrap_or_else(|_| "regret-system".to_string()),
            auth_password: env::var("AUTH_PASSWORD").ok().filter(|s| !s.is_empty()),
        }
    }
}
