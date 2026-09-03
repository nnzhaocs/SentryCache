# Kubernetes lifecycle event subscriber.
# Streams pod / deployment events (Scheduled, Initializing, Ready, Terminating, Failed,
# imageChanged) onto an asyncio.Queue consumed by prefetch / migration / version_tag.
