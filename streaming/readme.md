# Streaming Module

Instructions for this module will be added soon. 

In the meantime, start by navigating to the `pyflink` directory and running the following command:

```bash
docker-compose up
```

Flow of stream data: (Kafka + Flink + Postgres)
```
Kafka Topic         (raw stream)
      ↓
Flink Source Table  (temporary, streaming view)
      ↓
SQL transformation  (optional)
      ↓
Flink Sink Table    (temporary pointer to Postgres)
      ↓
Postgres            (persistent storage)
```

# Flink Watermark Key Comparisons

## 1. Event Time vs. Watermark
Determines if an event is on-time or late:

| Comparison | Condition | Meaning | Outcome |
|------------|-----------|---------|---------|
| **Late Event** | `event_time <= watermark` | Event arrived after Flink expected it | Processed only if within allowed lateness, otherwise dropped |
| **On-time Event** | `event_time > watermark` | Event arrived when expected | Processed normally, may advance watermark |

## 2. Watermark vs. Window End Time
Determines when windows are processed:

| Comparison | Condition | Window Status | Outcome |
|------------|-----------|---------------|---------|
| **Window Open** | `watermark <= window_end_time` | Still collecting events | Events continue accumulating, no results emitted |
| **Window Closed** | `watermark > window_end_time` | Finished collecting on-time events | Window fires, calculation performed, results emitted |