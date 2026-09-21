# Streaming Pipeline Report: EdNet-KT1 with PySpark Structured Streaming

## 1. Summary

This project simulates a live event stream from a large historical dataset and processes it with Spark Structured Streaming. A sample of 500 students is drawn from EdNet-KT1 (784,309 students, 5.3 GB), merged into one time-ordered log, and replayed as small files into a watched folder. A Spark job consumes the files and reports throughput per micro-batch and event counts per hourly event-time window.

The goal was a small, end-to-end, reproducible demonstration of a streaming pipeline (ingest, event-time windowing, checkpointing). It was not to produce learning analytics. The pipeline ran end to end: 218 files were fed, and Spark processed them as 200-row micro-batches with steadily advancing windows.

## 2. Data

| Item | Value |
|---|---|
| Source | EdNet-KT1: one CSV per student, `timestamp, solving_id, question_id, user_answer, elapsed_time` |
| Full size | 784,309 files, about 5.3 GB |
| Sample | 500 students (random seed 42), **43,574 rows** |
| Sample time span | 2017-07-01 to 2019-12-02 |
| Distinct questions | 9,639 |
| Rows per student | median 10, maximum 6,332 (heavily skewed) |

KT1 files have no `user_id` column, so it is taken from the filename (`u123.csv` becomes `u123`). `timestamp` is epoch milliseconds. EdNet-Content (`questions.csv` etc.) was downloaded but is not used by this version.

## 3. Architecture

```
784,309 per-user CSVs
        |  sampler_dev.ipynb: sample 500 users, tag user_id,
        |  merge, sort by timestamp
        v
data/sample/kt1_sample.csv   (43,574 rows, globally time-ordered)
        |  feeder_dev.ipynb: split into 200-row chunks,
        |  write one file every 2 s (atomic rename)
        v
data/stream_input/part-000000.csv ... part-000217.csv   (218 files)
        |  streaming_job_dev.ipynb: Spark readStream (CSV, explicit schema),
        |  one file per 5 s trigger
        v
Query 1: rows per micro-batch (foreachBatch)      -> console + data/output/batch_counts/
Query 2: count per 1-hour event-time window,
         1-day watermark, update mode             -> console + data/output/window_counts/
         (one Parquet file per batch in each folder;
          each query has its own checkpoint under checkpoints/)
```

The three stages live in three notebooks under `notebooks/`. `src/config.py` is the only Python module; it holds shared paths, the column order and default parameters.

## 4. Components and key design decisions

**Sampling and ordering.** Students are sampled with a fixed seed so runs are reproducible. Rows from all sampled students are merged and sorted by timestamp with a stable sort, so replaying the file in order approximates one shared timeline.

**Feeding.** The sorted sample is cut into consecutive 200-row chunks (218 files; the last has 174 rows). Each chunk is written to a hidden temporary name and then renamed into place, so Spark never sees a half-written file.

**Schema.** Spark reads CSV with an explicit schema and `header=true`. Spark matches those columns **by position, not by name**, so the schema order must equal the feeder's column order. Both are driven from one constant, `KT1_STREAM_COLUMNS`.

**Event time.** `timestamp / 1000` is converted to a timestamp column, and windowing uses the event's own time, not arrival time. Because the feeder replays in sorted order, the watermark advances with the data even though the dates span 2017 to 2019.

**Two queries, not one.** The per-batch row count is stateless. The windowed count is stateful across batches, so it needs its own streaming aggregation and its own checkpoint.

**Parameters.** 200 rows per file, 2 s between files, 5 s trigger, 1 file per trigger, 1-hour windows, 1-day watermark. All are defaults in `config.py`.

## 5. Results

All figures below were computed from the saved Parquet output of the final run.

- **Complete and lossless.** 218 batches (ids 0 to 217, none missing) were processed: 217 of 200 rows and a final one of 174, totalling **43,574 rows**, exactly the sample size.
- **Cumulative counts are consistent.** Taking the latest count of each hourly window and summing over all windows gives 43,574 again, so the running window totals lost and double-counted nothing.
- **Coverage.** 3,117 distinct hourly windows from 2017-07-01 to 2019-12-02 (about 29 months). 207 of them were updated in more than one batch because their rows straddled a batch boundary (3,324 window rows in total). Busiest windows: 2018-08-04 12:00 (111 events) and 2018-07-05 16:00 (104).
- **Windows per batch.** Between 5 and 31, median 15, sometimes days apart. The sample is sparse (43,574 rows over about 29 months), so 200 consecutive rows span days, not minutes.
- **Timing.** The full run took 18 minutes. Batches finished at a steady 5 s cadence (median gap 5.0 s, 95th percentile 5.1 s, maximum 5.4 s), i.e. the 5 s trigger was met throughout the saved run.
- **Time zone.** Spark prints window times in the machine's local time zone (UTC+3 here), so they are three hours ahead of UTC.
- **Not verified:** window finalisation. In update mode the output shows only changed windows, so watermark-driven state cleanup is not visible.

## 6. Problems found and how they were resolved

1. **Overlapping chunks.** The chunking loop stepped by 1 row instead of by the batch size, producing 43,574 files that each repeated 199 of the previous file's 200 rows. Symptom: one window crawled upward for 25+ batches and early counts roughly doubled. Fix: step by the batch size, giving 218 distinct chunks.
2. **Checkpoint deleted under live queries.** The reset step deletes `checkpoints/`. Running it after the queries had started crashed both with `FileNotFoundException`. Fix: reset is now a separate cell to run once, before the queries start, and the feed loop no longer resets.
3. **Positional schema.** Field order in the Spark schema must match the file column order exactly, or values land in the wrong columns without an error. Mitigated by the shared column constant.

## 7. Limitations

- Results are saved per batch as Parquet files, but there is no database, dashboard or downstream consumer. The saved files must be read and aggregated by hand (`pd.read_parquet`).
- The per-batch writers collect each batch to the driver with `toPandas()`. That is fine for a few dozen rows per batch and would not scale.
- The metric is throughput and counts. There is no per-student or per-question analysis, and the content tables are unused.
- Local single-machine Spark (`local[*]`) with a 1 GB driver heap. This demonstrates the streaming model, not cluster-scale behaviour.
- Rate mismatch: the feeder writes one file per 2 s and Spark reads one per 5 s, so Spark trails the feeder and finishes later.
- Only 500 of 784,309 students are used, and the per-student distribution is very skewed (median 10 rows, maximum 6,332).
- Reset safety is manual: nothing stops someone from running the reset cell while queries are live.

## 8. How to run

1. `sampler_dev.ipynb`: run top to bottom once (writes `data/sample/kt1_sample.csv`).
2. `feeder_dev.ipynb`: run up to and including the reset cell, which clears `data/stream_input/`, `checkpoints/` and `data/output/`. Do this **before** starting the queries.
3. `streaming_job_dev.ipynb`: run Step 0 through Step 6 to start both queries.
4. `feeder_dev.ipynb`: run the feed-loop cell.
5. `streaming_job_dev.ipynb`: run the `awaitAnyTermination()` cell to watch output (Spark UI at http://localhost:4040). It never returns on its own; interrupt it after batch 217, then run the cleanup cell to stop the queries.
6. Analyse the results with `pd.read_parquet("data/output/batch_counts")` and `pd.read_parquet("data/output/window_counts")`.

Never re-run the reset cell while the queries are running.

## 9. Possible next steps

- Join the stream to `questions.csv` for per-question accuracy and per-part activity.
- Raise `maxFilesPerTrigger` (or lower the feed interval) so consumption keeps pace with the feeder.
- Add a guard that refuses to reset while checkpoints were touched recently.
