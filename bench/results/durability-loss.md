# Durability Loss

| Backend | Mode | Enqueued before failure | Recoverable after restart | Lost |
|---|---|---:|---:|---:|
| Postgres | synchronous_commit=on | 200 | 200 | 0 |
| Postgres | synchronous_commit=off | 200 | 195 | 5 |
| BullMQ/Redis | appendfsync=everysec (default) | 200 | 99 | 101 |
| BullMQ/Redis | appendfsync=always | 200 | 200 | 0 |
