# Version-tagged replica sets (R3, paper §IV-D).
# Extracts the 12-char image-digest tag from each pod's container_statuses[].image_id,
# pushes it to the sidecar via env var, and partitions L2 keys per version to suppress stale reads.
