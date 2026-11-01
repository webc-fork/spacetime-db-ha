use openraft::{
    BasicNode,
    error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
};
use reqwest::{Certificate, Client};

use crate::raft::types::TypeConfig;

type C = TypeConfig;

/// Network transport for Raft inter-node communication via HTTP(S).
/// Each shard's Raft instance gets its own NetworkFactory with its shard_id,
/// producing URLs like `/raft/{shard_id}/append`.
pub struct NetworkFactory {
    client: reqwest::Client,
    use_tls: bool,
    shard_id: u64,
}

impl NetworkFactory {
    pub fn new(use_tls: bool, ca_cert_pem: Option<&[u8]>, shard_id: u64) -> Self {
        let mut builder = Client::builder();

        if let Some(ca_pem) = ca_cert_pem
            && let Ok(cert) = Certificate::from_pem(ca_pem)
        {
            builder = builder.add_root_certificate(cert);
        }

        Self {
            client: builder.build().unwrap_or_else(|_| Client::new()),
            use_tls,
            shard_id,
        }
    }
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new(false, None, 0)
    }
}

pub struct NetworkConnection {
    target: u64,
    addr: String,
    client: reqwest::Client,
    use_tls: bool,
    shard_id: u64,
}

impl NetworkConnection {
    /// Build a URL for a Raft RPC: `{scheme}://{addr}/raft/{shard_id}/{rpc}`
    fn raft_url(&self, rpc: &str) -> String {
        let scheme = if self.use_tls { "https" } else { "http" };
        format!("{}://{}/raft/{}/{}", scheme, self.addr, self.shard_id, rpc)
    }
}

impl RaftNetworkFactory<C> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            target,
            addr: node.addr.clone(),
            client: self.client.clone(),
            use_tls: self.use_tls,
            shard_id: self.shard_id,
        }
    }
}

impl RaftNetwork<C> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let url = self.raft_url("append");
        let resp: Result<AppendEntriesResponse<u64>, RaftError<u64>> = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let url = self.raft_url("vote");
        let resp: Result<VoteResponse<u64>, RaftError<u64>> = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let url = self.raft_url("snapshot");
        let resp: Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>> = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        resp.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}
