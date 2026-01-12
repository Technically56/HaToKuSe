# HaToKuSe

**HaToKuSe** is a distributed key-value storage system utilizing gRPC for internal node communication and a custom TCP protocol for client interactions. It implements a leader-follower architecture with a Consistent Hashing (Hash Ring) strategy for data distribution and replication.

## Authors
* Can Pusat Dincer
* Abdullah Efe Sekili

---

## Features
*   **Distributed Architecture**: Leader server manages a ring of scalable storage nodes.
*   **Consistent Hashing**: Efficient data distribution and node scalability.
*   **Replication**: Configurable replication tolerance to ensure data persistence even if nodes fail.
*   **Hybrid Communication**:
    *   **gRPC**: High-performance internal communication between Leader and Nodes.
    *   **TCP**: Simple text-based protocol for external clients.
*   **Heartbeat Mechanism**: Automatic detection and removal of dead nodes.
*   **Docker Ready**: Fully containerized with Docker Compose for easy deployment and scaling.

## Prerequisites
*   [Docker](https://docs.docker.com/get-docker/)
*   [Go 1.22+](https://go.dev/dl/) (Optional, for local development/testing)

## Installation & Setup

The recommended way to run HaToKuSe is using Docker Compose.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/Technically56/HaToKuSe.git
    cd HaToKuSe
    ```

2.  **Start the Cluster:**
    This command builds the images and spins up the Leader and 3 storage Nodes (default).
    ```bash
    docker compose up --build -d
    ```

3.  **Scale Nodes (Optional):**
    You can dynamically scale the number of storage nodes. For example, to run 5 nodes:
    ```bash
    docker compose up -d --scale node=5
    ```

## Configuration

### `config.yaml`
Defines the base networking ports for the system.
*   `leader.client_port`: Port exposed to external clients (Default: **6000**).
*   `leader.grpc_port`: Internal gRPC port for Leader-Node communication.

### `tolerance.conf`
Defines the replication factor.
*   **Default**: `4` (Data will be replicated to 4 distinct nodes).
*   If the number of active nodes is less than the tolerance, the system will attempt to replicate to as many as possible but report an error/warning.

## Usage

### Connecting via Client
You can interact with the Leader Server using `netcat` (nc) or any TCP client.

```bash
nc localhost 6000
```
*If using the Docker container `client-test`:*
```bash
docker compose exec client-test nc leader 6000
```

### Commands
Once connected, you can use the following commands:

*   **`SET <key> <value>`**
    *   Stores a value in the cluster.
    *   Example: `SET myfile HelloWorld`
*   **`GET <key>`**
    *   Retrieves a value.
    *   Example: `GET myfile`
*   **`NODES`**
    *   Lists all active storage nodes in the Hash Ring.
*   **`FILES`**
    *   Lists the file count stored on each node.
*   **`QUIT`**
    *   Closes the connection.

## Testing

The project includes a Go test suite for checking latency and throughput.

**Running Latency Test:**
```bash
go test -v -run TestLatency -args -packet_size=1024
```

**Running Throughput Benchmark:**
```bash
go test -v -run TestWriteThroughput
```

## Architecture Overview

1.  **Leader Server**:
    *   Acts as the entry point for clients.
    *   Maintains the Hash Ring.
    *   Routes `SET`/`GET` requests to appropriate Nodes via gRPC.
    *   Monitors Node health via heartbeats.

2.  **Node Server**:
    *   Stores data shards locally using a file-based manager.
    *   Registers with the Leader upon startup.
    *   Sends periodic heartbeats to the Leader.

## Stopping the System
To stop all containers and remove the network:
```bash
docker compose down
```
