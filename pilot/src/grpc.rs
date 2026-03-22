use tonic::{Request, Response, Status};

use regret_proto::regret_v1::pilot_service_server::PilotService;
use regret_proto::regret_v1::{RegisterRequest, RegisterResponse};

use crate::adapter::registry::AdapterRegistry;
use crate::storage::sqlite::SqliteStore;

pub struct PilotServiceImpl {
    pub registry: AdapterRegistry,
    pub sqlite: SqliteStore,
}

#[tonic::async_trait]
impl PilotService for PilotServiceImpl {
    async fn register_adapter(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();

        // Validate hypothesis exists
        match self.sqlite.get_hypothesis(&req.hypothesis_id).await {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Ok(Response::new(RegisterResponse { accepted: false }));
            }
            Err(e) => {
                return Err(Status::internal(format!("database error: {e}")));
            }
        }

        // Register adapter
        self.registry
            .register(&req.hypothesis_id, &req.adapter_name, &req.grpc_addr)
            .await;

        Ok(Response::new(RegisterResponse { accepted: true }))
    }
}
