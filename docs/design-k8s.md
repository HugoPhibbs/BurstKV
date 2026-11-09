# BurstKV on Kubernetes: design

NB: Some sections are marked as preliminary, treat these as works in progress

## Goal

BurstKV is a scale-to-zero Redis service.

- Each DB has a max memory chosen by the user (e.g. 256 MB to 1 GB). That memory is guaranteed, so one DB can't affect its neighbours.
- A DB with no traffic for a while is paused to object storage. The next connection resumes it.
- Removing a VM must not disconnect clients.
- Nothing may depend on a particular cloud. Local development runs with minikube

## Summary

The worker layer (worker process, Docker CLI calls, port pool, gRPC) is replaced by Kubernetes, with one pod per DB.

- **The controller** becomes a Kubernetes controller written with `client-go`, and keeps the existing HTTP API and Redis proxy.
- **Kubernetes** handles running pods, placing them, replacing warm pods, and adding or removing VMs.
- **BurstKV code** handles the proxy, snapshots, the idle reaper and live migration.

## Components

1. **Controller.** One Go binary, running as one replica. It holds the HTTP API, the Redis proxy, the idle reaper and the Kubernetes client.
2. **DB pod.** The Redis image plus a small Go entrypoint (`cmd/dbpod`). It has an RPC interface for saving, loading, and ensures a kill signal from k8s (rebalancing to switch a node), first saves a snapshot.
3. **Kubernetes config** (`deploy/`):
   - a DaemonSet that keeps one warm pod per node,
   - a Deployment of placeholder pods,
   - the controller's Deployment, Service and RBAC,
   - scheduler and autoscaler settings.
4. **Object storage.** Any S3-compatible store (MinIO in dev). Each snapshot is stored at `<db-id>/dump.rdb`.

## Who does what

**Kubernetes (configuration only)**
- **Running DB pods:** starts them, restarts them if they crash, and enforces their memory limits.
- **Resizing:** changes a running pod's memory in place, without a restart.
- **Warm pool:** the DaemonSet keeps one tiny warm pod on every node and replaces each one that gets claimed.
- **Headroom:** the placeholder Deployment keeps N low-priority placeholder pods, sized at the largest tier.
- **VMs:** the cluster autoscaler adds VMs when pods can't fit and removes VMs that are underused.
- **Networking:** every pod gets its own IP. A LoadBalancer Service exposes the proxy and the HTTP API.

**BurstKV code (no Kubernetes involved)**
- The HTTP API.
- The proxy.
- The DB pod's entrypoint, which loads and saves snapshots and sets `maxmemory`.
- The live migration steps.
- The idle reaper.

**BurstKV code that drives Kubernetes**
- Claiming a pod for a DB.
- Pausing and deleting DBs.
- Tracking which DB is on which pod, by watching pod events.
- Answering preStop calls, which may trigger a migration.

## DB pod

**Resources**
- Set memory request = limit = the DB's max memory. Set CPU request = limit as well, which gives the pod the Guaranteed QoS class.
- A pod's QoS class can't change during a resize, so always resize requests and limits together.
- Set Redis `maxmemory` to about 80% of the memory limit. The rest covers Redis overhead and the fork during save or replication.

**Entrypoint**
- The pod starts idle, with Redis not running.
- It has a RPC server running, that handles it's key lifecycle events:
  - `Load(dbID, maxMemory, snapshotURL, create=True)`: Creates a new DB if create is set to true. The snapshotURL is presigned so the pod can automatically save its state on a sigterm (e.g. if k8s has decided to move it). If create is set to false, then the snapshot is read, and the db is restarted from the snapshot 
  - `Save` saves the snapshot to memory. This is intended to save the state when doing a migration (as backup), and for pausing the DB
- The controller generates presigned URLs for one DB at a time, so pods never hold bucket credentials.

**Labels and annotations** (preliminary)
- Labels: `burstkv/role=warm|db` and `burstkv/db-id=<id>`.
- Annotation: `cluster-autoscaler.kubernetes.io/safe-to-evict: "true"`. Without it, the autoscaler won't remove nodes that host pods with no owning controller or with emptyDir volumes, and claimed DB pods have both.

**Termination** (preliminary)
- Set `terminationGracePeriodSeconds` per pod, from the DB's size, so a migration of that size can finish. The autoscaler's `--max-graceful-termination-sec` is a hard cap on this.

**Network:** a NetworkPolicy allows only the controller to reach DB pods. (Outside Redis requests are routed through the controller proxy)

## Flows

### Claim a pod for a DB

**Warm path**
1. Candidate nodes are schedulable, don't have the `ToBeDeletedByClusterAutoscaler` taint, and have an unclaimed warm pod.
2. For each candidate, compute free memory as allocatable minus the requests of all non-placeholder pods. Pick the fullest node that still has room for the DB, so DBs are packed together.
3. If that room is currently taken by placeholder pods, delete one first. An in-place resize won't evict a placeholder by itself.
4. Relabel the warm pod with `role=db` and `db-id=<id>`. The DaemonSet then releases it and starts a replacement on the same node.
5. Resize the pod's memory and CPU to the DB's size through the `pods/resize` subresource, and wait until the resize has been applied.
6. Call `Load` with a presigned GET URL. Leave the URL out for a new DB or a migration target.

**Cold path**

When no warm pod fits, create a full-size DB pod and let the scheduler place it. The scheduler evicts placeholder pods if it needs room. Then call `/load`.

### Create

1. Generate a DB ID.
2. Claim a pod with no DB currently running
3. Record the DB as Running.

### Resume

This runs when a client connects to a paused DB.
1. The proxy reads `AUTH` and finds the DB paused.
2. The controller claims a pod and loads the snapshot.
3. The proxy dials the pod's IP.

Only one resume runs per DB. Other connections to the same DB wait for it to finish.

### Pause

The idle reaper pauses a DB after 5 minutes with no traffic.
1. The proxy holds new traffic for the DB.
2. The controller calls `Save` with a presigned PUT URL.
3. The controller marks the DB Paused and deletes its pod, which is then recreated by k8s (to ensure a clean slate for a new tenant)

### Delete

Delete the DB's pod if one is running, then delete its snapshot.

### Scale up

- **Placeholder pod logic is preliminary, and not fully investigated**
- **New nodes come pre-warmed.** The warm-pod DaemonSet automatically puts a warm pod on every new node (or several), this should be configurable

### Scale down

**Autoscaler settings**
- `--scale-down-utilization-threshold=0.2`
- `--scale-down-unneeded-time=5m`

**What happens**
1. The autoscaler taints the node and evicts its pods.
2. Each DB pod's Save is called
3. The controller live-migrates that DB to another node.
4. The hook returns and the pod exits.
5. Once the node is empty, the autoscaler removes it.

### Live migration (prelimary)

Redis 6.2 or later is required for `FAILOVER`.d

## Open decisions and risks

- **Where paused-DB records live:** rebuild them from the snapshot bucket (store the size in object metadata), use one CRD per DB, or use a database.
- **Fork memory:** during a save or replication, copy-on-write can push a write-heavy DB towards 2× its memory. 80% `maxmemory` may not cover that.
- **Concurrent migrations:** the autoscaler evicts a node's pods roughly at once, so many migrations run in parallel.
- **Managed clusters may lock the default scheduler.** If you can't change its scoring, run a second scheduler profile and set `schedulerName` on DB pods, or rely on the claim logic alone.
