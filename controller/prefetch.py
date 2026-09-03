# Container-replication-aware prefetch (R1, paper §IV-B).
# Triggered on Pod Scheduled; ranks sibling LFU snapshots by score = f_hat(k) / size(k),
# selects same-node-preferred source, transfers via DUMP+RESTORE inside an 8s budget.
