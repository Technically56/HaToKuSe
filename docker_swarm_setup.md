# Docker Swarm Setup Guide for HaToKuSe

This guide explains how to deploy the **HaToKuSe** project using Docker Swarm. The project includes a `docker-stack.yaml` file specifically designed for Swarm deployments.

## Prerequisites

- **Docker** installed (v1.13+).
- **Docker Swarm** initialized.

If you haven't initialized Swarm mode yet, run:
```bash
docker swarm init
```
*(If you have multiple network interfaces, you may need to specify `--advertise-addr <YOUR_IP>`)*

## Step 1: Build the Docker Image

The stack configuration relies on the `hatokuse:latest` image. You need to build this image locally before deploying.

```bash
docker build -t hatokuse:latest .
```

> **Note for Multi-Node Swarms**: If you are deploying to a multi-node swarm, you must push this image to a registry (e.g., Docker Hub or a local registry) so all nodes can pull it. For a local single-node swarm, the local build is sufficient.

## Step 1.5: Configure Node Labels

To ensure that the **leader** and **node** (worker) services run on specific machines, you need to label your nodes.

1.  List your nodes to get their hostnames:
    ```bash
    docker node ls
    ```
2.  Assign the `leader` label to the machine that should run the leader:
    ```bash
    docker node update --label-add type=leader <HOSTNAME_OF_LEADER_MACHINE>
    ```
3.  Assign the `worker` label to the machine(s) that should run the worker nodes:
    ```bash
    docker node update --label-add type=worker <HOSTNAME_OF_WORKER_MACHINE>
    ```

## Step 2: Deploy the Stack

Use the provided `docker-stack.yaml` to deploy the application as a stack. We'll verify the file exists first.

```bash
ls docker-stack.yaml
```

Deploy the stack (we'll name it `hatokuse_stack`):

```bash
docker stack deploy -c docker-stack.yaml hatokuse_stack
```

Configuration highlights from `docker-stack.yaml`:
- **Leader Service**: 1 replica, runs on a manager node, exposes port `6000`.
- **Node Service**: 7 replicas (default), connects to the leader via the overlay network `hatokuse_net`.
- **Restart Policy**: Containers restart automatically on failure.

## Step 3: Verify Deployment

Check the status of your services:

```bash
docker service ls
```

You should see two services: `hatokuse_stack_leader` and `hatokuse_stack_node`.

To check the individual tasks (containers) for the leader:
```bash
docker service ps hatokuse_stack_leader
```

To check the nodes:
```bash
docker service ps hatokuse_stack_node
```

### Verifying Multi-Node Distribution

To see which physical machine (node) each container is running on, look at the **NODE** column in the output of the commands above.

To verify your swarm nodes status:
```bash
docker node ls
```

## Step 4: Scaling Nodes

One of the benefits of Docker Swarm is easy scaling. To change the number of node replicas (e.g., from 7 to 10):

```bash
docker service scale hatokuse_stack_node=10
```

## Step 5: Viewing Logs

To view the logs of the leader service:

```bash
docker service logs -f hatokuse_stack_leader
```

To view logs for all nodes (this can be noisy):
```bash
docker service logs -f hatokuse_stack_node
```

## Step 6: Cleanup

To remove the stack and stop all containers:

```bash
docker stack rm hatokuse_stack
```

This will remove the services and the `hatokuse_net` network.

## Troubleshooting

- **Image not found**: Ensure you built the image `hatokuse:latest` and that it is available.
- **Pending services**: If services are stuck in `Pending`, check if you have enough resources or if the placement constraints (runs on manager) are met.
