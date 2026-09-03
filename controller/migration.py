# Container-departure-aware migration + crash recovery (R4, paper §IV-C).
# preStop -> classify (shared-critical / shared-surplus / private-hot / private-cold)
# -> priority-queue drain into surviving siblings within the K8s grace period.
# on_pod_failed -> repair Sk for tracked keys whose holder set fell below REPLICA_LOWER_BOUND.
