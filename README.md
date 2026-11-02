# Kafka - Kasten Blueprint

> [!WARNING]
> This blueprint is **not supported by Veeam**. It is provided as an example only, and it may require some tuning to work in your specific environment (Kafka distribution, topology, storage, RBAC). Test it thoroughly before relying on it to protect production data.

Application-consistent backup of an Apache Kafka KRaft cluster with Veeam Kasten. The repo contains the blueprint, a small Helm chart to deploy a 3-node Kafka test cluster with the official `apache/kafka` image, and scripts that seed and verify deterministic fake data.

## Chosen pattern

**Quiesce by scale-down** - Kafka has no command to freeze writes or flush every broker at once, and the PVCs of the brokers are snapshotted one by one. A hot (crash-consistent) snapshot captures each broker at a slightly different moment, with open segments that need log recovery at restart. The `backupPrehook` scales the StatefulSet to 0 and waits for every pod to terminate: each node shuts down cleanly (segments flushed and closed, clean shutdown marker written). Veeam Kasten then snapshots all the PVCs in the same stopped state, and the `backupPosthook` scales Kafka back up.

- Kafka is **unavailable** from the prehook until the posthook has restarted the cluster. On the validation cluster this lasted 26 seconds per backup (clean stop 4 s, snapshots, restart 22 s), the whole backup action took 44 seconds. Producers and consumers retry and resume.
- The StatefulSet spec is captured while Kafka is stopped, so a restore brings it back with `replicas: 0`. The `restorePosthook` scales it back to the count saved in the `kasten-kafka/original-replicas` annotation.
- Kasten remains the data mover: incremental snapshots, export, immutability.
- Why not the alternatives: backing up a MirrorMaker2 replica (fence a replica pattern, see [michaelcourcy/kafka-kasten](https://github.com/michaelcourcy/kafka-kasten)) avoids the downtime but needs Strimzi and a second cluster. Hot snapshots plus health checks avoid both but do not give a consistent point across brokers.

## Blueprint actions

| Action | Phase | Function | Purpose |
|---|---|---|---|
| `backupPrehook` | `stopKafka` | `KubeTask` (kasten-io, `bitnami/kubectl`) | Save the replica count in the `kasten-kafka/original-replicas` annotation, scale the StatefulSet to 0, wait until every pod is deleted |
| `backupPosthook` | `startKafka` | `KubeTask` (kasten-io, `bitnami/kubectl`) | Scale back to the saved replica count, wait for the rollout |
| `restorePosthook` | `startKafka` | `KubeTask` (kasten-io, `bitnami/kubectl`) | Scale to the saved replica count if the restored spec differs, wait for the rollout |
| `restorePosthook` | `checkKafka` | `KubeExec` (`<sts>-0`, container `kafka`) | Wait until every broker is registered and no partition is under-replicated, print the message count per topic |

No `restorePrehook`: Veeam Kasten already scales the workload down before restoring the PVCs. (`restorePrehook` is not triggered in Kasten <= 8.5.x anyway.)

The prehook is idempotent: on a retry where the StatefulSet is already at 0, it keeps the replica count saved by the first attempt.

## Prerequisites

- A CSI StorageClass with a `VolumeSnapshotClass` (the default `gp2` in-tree class fails with `cannot find CSI PersistentVolumeSource`).
- A location profile, set in the policy as the **location profile for Kanister actions** (`backupParameters.profile`). Without it the backup fails with `No Kanister profile found` before the prehook runs.
- The `kanister-svc` service account in `kasten-io` with RBAC to get, annotate and scale StatefulSets in the application namespace (cluster-admin in a default Veeam Kasten install).

## Test workload deployment

`deploy-kafka.sh` installs the local chart [chart/kafka-kraft](chart/kafka-kraft) (3 combined broker + controller nodes, KRaft, `apache/kafka` image) and seeds deterministic fake data. It prompts for the namespace, the Helm release name and the StorageClass (defaults: `kafka`, `kafka`, the cluster default StorageClass).

```bash
./deploy-kafka.sh
```

Main chart values ([values.yaml](chart/kafka-kraft/values.yaml)):

| Value | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` | `docker.io/apache/kafka` / `4.2.0` | Official multi-arch image |
| `replicaCount` | `3` | Builds the KRaft voter list, change it with `helm upgrade`, not `kubectl scale` |
| `clusterId` | fixed example ID | Set your own for anything beyond a test |
| `persistence.storageClass` / `persistence.size` | cluster default / `2Gi` | Must support CSI snapshots |
| `heapOpts` | `-Xms512m -Xmx512m` | Broker JVM heap |

The client listener is `PLAINTEXT` on port 9092, without authentication: it is a test cluster, do not expose it outside the namespace.

Seeded data ([fake-data.sh](fake-data.sh)), 160 messages:

| Topic | Partitions | Messages | Content |
|---|---|---|---|
| `shop-customers` | 1 (compacted) | 10 | Customers keyed by `cust-01`...`cust-10`: name, city, email |
| `shop-orders` | 3 | 100 | Orders keyed by `ord-001`...`ord-100`: customer, product, quantity, total, date |
| `shop-events` | 1 | 50 | Customer events keyed by `evt-001`...`evt-050`: login, page_view, add_to_cart, checkout, logout |

The consumer group `billing-service` has committed offset 10 on every `shop-orders` partition (30 messages consumed), to check that consumer offsets survive the restore. The topic replication factor is 3.

The script can be rerun at any time: it deletes and recreates the `shop-*` topics and the consumer group.

`verify-data.sh` checks partition counts, message counts, the content of every message, the `compact` cleanup policy, the committed offsets of `billing-service` and that no partition is under-replicated. It exits non-zero if anything is wrong:

```bash
./verify-data.sh
```

## Applying the blueprint

```bash
kubectl apply -f kafka-blueprint.yaml
# or, without the BlueprintBinding:
kubectl annotate statefulset/kafka -n kafka kanister.kasten.io/blueprint=kafka-bp
```

The BlueprintBinding attaches the blueprint to every StatefulSet labelled `app.kubernetes.io/name=kafka-kraft` that has no `kanister.kasten.io/blueprint` annotation.

## End-to-end test

1. `./deploy-kafka.sh`, then `./verify-data.sh` to confirm the baseline is `All test data restored correctly`.
2. On-demand policy on namespace `kafka` with backup + export, then RunAction -> `Complete`:
   ```yaml
   apiVersion: config.kio.kasten.io/v1alpha1
   kind: Policy
   metadata:
     name: kafka-bp-test
     namespace: kasten-io
   spec:
     frequency: "@onDemand"
     selector:
       matchExpressions:
         - key: k10.kasten.io/appNamespace
           operator: In
           values: ["kafka"]
     actions:
       - action: backup
         backupParameters:
           profile:
             name: <location-profile>
             namespace: kasten-io
       - action: export
         exportParameters:
           frequency: "@onDemand"
           profile:
             name: <location-profile>
             namespace: kasten-io
           exportData:
             enabled: true
   ```
3. Logs: `kubectl logs -n kasten-io deploy/kanister-svc --since=10m | grep -E "Stopping|Kafka stopped|Starting"`
4. Wipe the data:
   ```bash
   kubectl exec -n kafka kafka-0 -c kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic 'shop-.*'
   kubectl exec -n kafka kafka-0 -c kafka -- /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --delete --group billing-service
   ```
   `./verify-data.sh` now fails.
5. Restore from the restore point -> `restorePosthook` prints `Kafka restored - 3 brokers, no under-replicated partitions` and the message count per topic (`kubectl logs -n kasten-io deploy/kanister-svc --since=10m | grep -A5 "Kafka restored"`).
6. `./verify-data.sh` -> `All test data restored correctly`.

Cleanup (`remove-kafka.sh` uninstalls the release, deletes the namespace with its PVCs and optionally its restore points):
```bash
./remove-kafka.sh
```

## Validated versions

| Component | Version |
|---|---|
| Kubernetes | v1.33.6 (K3s) |
| Veeam Kasten | 9.0.8 |
| Storage | OpenEBS ZFS LocalPV (`zfs.csi.openebs.io`), restore from exported restore point |
| Helm chart | kafka-kraft 1.0.0 (this repo) |
| Kafka | apache/kafka 4.2.0 (KRaft) |

## Notes

- **Failed backup after the prehook**: if the backup fails between the prehook and the posthook (snapshot error for example), Veeam Kasten does not run the `backupPosthook` and Kafka stays at 0 replicas. Rerun the policy, or scale back manually:
  ```bash
  kubectl scale statefulset/kafka -n kafka --replicas=$(kubectl get statefulset kafka -n kafka -o jsonpath="{.metadata.annotations['kasten-kafka/original-replicas']}")
  ```
- **Other Kafka distributions** (Strimzi, Confluent for Kubernetes): their pods are not managed by a plain StatefulSet scale (Strimzi uses `StrimziPodSet`), so this blueprint does not apply as is.
- **Separate broker and controller pools**: stop the brokers first, then the controllers, and bind the blueprint to each StatefulSet; not covered here.
- **Image**: the hooks use `docker.io/bitnami/kubectl:latest` (a rolling tag) for the `KubeTask` phases and the Kafka CLI shipped in `apache/kafka` (`/opt/kafka/bin`) for `checkKafka`. No custom image is required.
- **OpenEBS ZFS LocalPV**: restore from the exported restore point, not from the local snapshot. An in-place restore deletes the original PVC first, which destroys the ZFS dataset together with its snapshots.

## Changelog

| Version | Changes |
|---|---|
| 1.0.0 | Initial release: scale-down quiesce blueprint + BlueprintBinding, `kafka-kraft` Helm chart (3-node KRaft, `apache/kafka`), `deploy-kafka.sh` (install + fake data), `verify-data.sh` (post-restore data check), `remove-kafka.sh` (cleanup), support disclaimer. |
