use std::{collections::BTreeMap, io::ErrorKind, path::Path, process::Command};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use openraft::BasicNode;
use reqwest::Client;
use serde_json::Value;

#[derive(Parser)]
#[command(name = "rtdb", about = "RaftTimeDB cluster management CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show cluster status
    Status {
        /// RaftTimeDB node address
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        addr: String,
    },

    /// Add a node to the cluster
    AddNode {
        /// Node ID to add
        #[arg(long)]
        node_id: u64,
        /// Node's Raft address (host:port)
        #[arg(long)]
        addr: String,
        /// Existing cluster node to send the request to
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        cluster: String,
    },

    /// Remove a node from the cluster
    RemoveNode {
        /// Node ID to remove
        #[arg(long)]
        node_id: u64,
        /// Existing cluster node to send the request to
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        cluster: String,
    },

    /// Initialize a new cluster
    Init {
        /// Node IDs and addresses (format: "id=host:port", e.g., "1=node1:4001")
        #[arg(long, num_args = 1..)]
        nodes: Vec<String>,
    },

    /// Deploy a SpacetimeDB module to all nodes in the cluster.
    /// Runs `spacetime publish` against each SpacetimeDB instance.
    Deploy {
        /// Path to the SpacetimeDB module directory
        #[arg(long)]
        module: String,
        /// Database name (defaults to module directory name)
        #[arg(long)]
        name: Option<String>,
        /// SpacetimeDB server addresses (e.g., "http://localhost:5001")
        #[arg(long, num_args = 1.., default_values_t = vec![
            "http://localhost:5001".to_string(),
            "http://localhost:5002".to_string(),
            "http://localhost:5003".to_string(),
        ])]
        servers: Vec<String>,
    },

    /// Create a new shard (Raft group)
    CreateShard {
        /// Shard ID to create
        #[arg(long)]
        shard_id: u64,
        /// Existing cluster node to send the request to
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        cluster: String,
    },

    /// Add a module -> shard route
    AddRoute {
        /// Module/database name
        #[arg(long)]
        module: String,
        /// Target shard ID
        #[arg(long)]
        shard_id: u64,
        /// Existing cluster node to send the request to
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        cluster: String,
    },

    /// Remove a module -> shard route
    RemoveRoute {
        /// Module/database name to unroute
        #[arg(long)]
        module: String,
        /// Existing cluster node to send the request to
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        cluster: String,
    },

    /// List all shards and routes
    ListShards {
        /// RaftTimeDB node address
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        addr: String,
    },

    /// Show status for a specific shard
    ShardStatus {
        /// Shard ID to query
        #[arg(long)]
        shard_id: u64,
        /// RaftTimeDB node address
        #[arg(long, default_value = "http://127.0.0.1:4001")]
        addr: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::new();

    match cli.command {
        Commands::Status { addr } => {
            let url = format!("{}/cluster/status", addr);
            let resp = client
                .get(&url)
                .send()
                .await
                .context("Failed to connect to node")?;

            let status: Value = resp
                .json()
                .await
                .context("Failed to parse status response")?;

            println!("Cluster Status:");
            println!("  Node ID:    {}", status["node_id"]);
            println!("  State:      {}", status["state"]);
            println!("  Leader:     {}", status["current_leader"]);
            println!("  Term:       {}", status["current_term"]);
            println!("  Applied:    {}", status["last_applied"]);
            println!("  Membership: {}", status["membership"]);
            if let Some(shards) = status["active_shards"].as_array() {
                let shard_ids: Vec<String> = shards.iter().map(|v| v.to_string()).collect();
                println!("  Shards:     [{}]", shard_ids.join(", "));
            }
        }

        Commands::Init { nodes } => {
            // Parse "id=host:port" entries into a BTreeMap
            let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();
            let mut first_addr: Option<String> = None;

            for node_spec in &nodes {
                let (id_str, addr) = node_spec.split_once('=').context(format!(
                    "Invalid node format '{}', expected 'id=host:port'",
                    node_spec
                ))?;

                let id: u64 = id_str
                    .parse()
                    .context(format!("Invalid node ID '{}'", id_str))?;

                if first_addr.is_none() {
                    first_addr = Some(format!("http://{}", addr));
                }

                members.insert(
                    id,
                    BasicNode {
                        addr: addr.to_string(),
                    },
                );
            }

            let target = first_addr.context("No nodes specified")?;
            let url = format!("{}/cluster/init", target);

            println!("Initializing cluster via {}...", url);
            for (id, node) in &members {
                println!("  Node {}: {}", id, node.addr);
            }

            let resp = client
                .post(&url)
                .json(&members)
                .send()
                .await
                .context("Failed to connect to node")?;

            if resp.status().is_success() {
                println!("Cluster initialized successfully!");
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Initialization failed: {}", body);
            }
        }

        Commands::AddNode {
            node_id,
            addr,
            cluster,
        } => {
            let url = format!("{}/cluster/add-node", cluster);
            println!("Adding node {} ({}) via {}...", node_id, addr, cluster);

            let resp = client
                .post(&url)
                .json(&serde_json::json!({
                    "node_id": node_id,
                    "addr": addr,
                }))
                .send()
                .await
                .context("Failed to connect to cluster")?;

            if resp.status().is_success() {
                println!("Node {} added successfully!", node_id);
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Add node failed: {}", body);
            }
        }

        Commands::Deploy {
            module,
            name,
            servers,
        } => {
            let db_name = name.unwrap_or_else(|| {
                Path::new(&module)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| "my_module".to_string())
            });

            println!(
                "Deploying module '{}' from {} to {} node(s)...",
                db_name,
                module,
                servers.len()
            );

            let mut failed = 0;
            for server in &servers {
                print!("  Publishing to {}... ", server);
                let result = Command::new("spacetime")
                    .args(["publish", "--server", server, &module, &db_name])
                    .output();

                match result {
                    Ok(output) if output.status.success() => {
                        println!("OK");
                    }
                    Ok(output) => {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        println!("FAILED: {}", stderr.trim());
                        failed += 1;
                    }
                    Err(e) => {
                        if e.kind() == ErrorKind::NotFound {
                            anyhow::bail!(
                                "spacetime CLI not found. Install it: https://spacetimedb.com/install"
                            );
                        }
                        println!("FAILED: {}", e);
                        failed += 1;
                    }
                }
            }

            if failed > 0 {
                anyhow::bail!(
                    "{} node(s) failed. All nodes must have the same module.",
                    failed
                );
            }
            println!("Module '{}' deployed to all nodes!", db_name);
        }

        Commands::RemoveNode { node_id, cluster } => {
            let url = format!("{}/cluster/remove-node", cluster);
            println!("Removing node {} via {}...", node_id, cluster);

            let resp = client
                .post(&url)
                .json(&serde_json::json!({
                    "node_id": node_id,
                }))
                .send()
                .await
                .context("Failed to connect to cluster")?;

            if resp.status().is_success() {
                println!("Node {} removed successfully!", node_id);
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Remove node failed: {}", body);
            }
        }

        Commands::CreateShard { shard_id, cluster } => {
            let url = format!("{}/cluster/shards/create", cluster);
            println!("Creating shard {} via {}...", shard_id, cluster);

            let resp = client
                .post(&url)
                .json(&serde_json::json!({
                    "shard_id": shard_id,
                }))
                .send()
                .await
                .context("Failed to connect to cluster")?;

            if resp.status().is_success() {
                println!("Shard {} created successfully!", shard_id);
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Create shard failed: {}", body);
            }
        }

        Commands::AddRoute {
            module,
            shard_id,
            cluster,
        } => {
            let url = format!("{}/cluster/shards/route", cluster);
            println!(
                "Adding route {} -> shard {} via {}...",
                module, shard_id, cluster
            );

            let resp = client
                .post(&url)
                .json(&serde_json::json!({
                    "module_name": module,
                    "shard_id": shard_id,
                }))
                .send()
                .await
                .context("Failed to connect to cluster")?;

            if resp.status().is_success() {
                println!("Route {} -> shard {} added!", module, shard_id);
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Add route failed: {}", body);
            }
        }

        Commands::RemoveRoute { module, cluster } => {
            let url = format!("{}/cluster/shards/route", cluster);
            println!("Removing route for {} via {}...", module, cluster);

            let resp = client
                .delete(&url)
                .json(&serde_json::json!({
                    "module_name": module,
                }))
                .send()
                .await
                .context("Failed to connect to cluster")?;

            if resp.status().is_success() {
                println!("Route for {} removed!", module);
            } else {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Remove route failed: {}", body);
            }
        }

        Commands::ListShards { addr } => {
            let url = format!("{}/cluster/shards", addr);
            let resp = client
                .get(&url)
                .send()
                .await
                .context("Failed to connect to node")?;

            let data: Value = resp.json().await.context("Failed to parse response")?;

            println!("Active Shards:");
            if let Some(shards) = data["shards"].as_array() {
                for shard in shards {
                    println!("  Shard {}", shard);
                }
            }

            println!("\nRoutes:");
            if let Some(routes) = data["routes"].as_object() {
                if routes.is_empty() {
                    println!("  (none — all modules go to shard 0)");
                }
                for (module, shard) in routes {
                    println!("  {} -> shard {}", module, shard);
                }
            }
        }

        Commands::ShardStatus { shard_id, addr } => {
            let url = format!("{}/cluster/shards/{}/status", addr, shard_id);
            let resp = client
                .get(&url)
                .send()
                .await
                .context("Failed to connect to node")?;

            if !resp.status().is_success() {
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("Shard status failed: {}", body);
            }

            let status: Value = resp
                .json()
                .await
                .context("Failed to parse status response")?;

            println!("Shard {} Status:", shard_id);
            println!("  State:      {}", status["state"]);
            println!("  Leader:     {}", status["current_leader"]);
            println!("  Term:       {}", status["current_term"]);
            println!("  Applied:    {}", status["last_applied"]);
            println!("  Membership: {}", status["membership"]);
        }
    }

    Ok(())
}
