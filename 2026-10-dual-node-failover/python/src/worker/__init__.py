"""Worker: one of two identical processes that never talk to each other. MODE=hothot reads and processes every event
(like the other worker does); MODE=dispatch takes tasks the upstream leases to it."""
