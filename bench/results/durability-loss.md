# Durability Loss

| Backend | Mode | Enqueued before failure | Recoverable after restart | Lost |
|---|---|---:|---:|---:|
| Postgres | synchronous_commit=on | 200 | 200 | 0 |
| Postgres | synchronous_commit=off | 200 | 198 | 2 |
| BullMQ/Redis | appendfsync=everysec (default) | 200 | 110 | 90 |
| BullMQ/Redis | appendfsync=always | 200 | 110 | 90 |
