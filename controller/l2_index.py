# L2 metadata index client.
# Owns the Redis key schema (replicas:{ver}:{key}, tracked_keys:{ver}, holders:{pod}, nodemap)
# and exposes the read/write helpers consumed by prefetch / migration / version_tag / lookup.
