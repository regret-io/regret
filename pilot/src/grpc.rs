use tonic::{Request, Response, Status};

use regret_proto::regret_v1::pilot_service_server::PilotService;
use regret_proto::regret_v1::{RegisterRequest, RegisterResponse};

use crate::storage::sqlite::SqliteStore;

pub struct PilotServiceImpl {
    pub sqlite: SqliteStore,
}

#[tonic::async_trait]
impl PilotService for PilotServiceImpl {
    async fn register_adapter(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let req = request.into_inner();

        tracing::info!(
            hypothesis_id = %req.hypothesis_id,
            adapter_name = %req.adapter_name,
            grpc_addr = %req.grpc_addr,
            "adapter registration received"
        );

        // For now, just acknowledge. Adapter definitions are managed via HTTP API.
        // This RPC serves as a liveness signal from running adapter instances.
        Ok(Response::new(RegisterResponse { accepted: true }))
    }
}
