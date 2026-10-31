"""Upstream: where the work comes from. An event log both hot-hot workers read in full, and a task queue that leases
each task to one worker. In production this would be Kafka, a message queue or a job table, itself replicated."""
