# Streaming EdNet-KT1 with PySpark Structured Streaming

Part 2 (Big Data Processing) of the *Cloud Computing and Big Data* course project at VILNIUS TECH / MERIT. This repository contains Part 2 only.

The project replays a sample of the EdNet-KT1 student-interaction logs as a live event stream and processes it with Spark Structured Streaming on a single machine. It demonstrates streaming ingestion, event-time windowing, watermarking and checkpointing at a reduced scale. It is **not** a learning-analytics project: the only metrics computed are event counts.

## Contents

1. [What the project does and why](#1-what-the-project-does-and-why)
2. [Data](#2-data)
3. [Architecture and data flow](#3-architecture-and-data-flow)
4. [Big-data concepts demonstrated](#4-big-data-concepts-demonstrated)
5. [Key design decisions](#5-key-design-decisions)
6. [Repository layout](#6-repository-layout)
7. [How to run](#7-how-to-run)
8. [Configuration](#8-configuration)
9. [Results](#9-results)
10. [Problems encountered](#10-problems-encountered)
11. [Limitations](#11-limitations)
12. [Possible next steps](#12-possible-next-steps)

## 1. What the project does and why

A stream of student answers is simulated from historical data. A sampler builds one time-ordered event log from 500 students, a feeder drops that log into a watched folder as small files at a fixed pace, and a Spark Structured Streaming job consumes the files as they arrive. The job produces two outputs, both saved to disk after every micro-batch:

- the number of rows in each micro-batch (throughput), and
- a running count of events per one-hour event-time window.

**Why streaming rather than batch.** Interaction events arrive continuously and are timestamped when they happen. Streaming forces the questions a batch job avoids: which time an event belongs to (event time versus arrival time), how long to wait for late data (watermark), how to keep state across micro-batches, and how to recover after a failure (checkpoints).

**Relation to Part 1.** The pipeline is a miniature of the event-driven architecture designed in Part 1: an event producer (the feeder), a landing zone that plays the role a message broker or topic would play in a deployed system (`data/stream_input/`), a stream processor (the Spark job) and sinks (Parquet files).

**Scope.** Everything runs locally (`local[*]`), with no cluster or cloud service, on 500 of the 784,309 students in the dataset.

## 2. Data

| Item | Value |
|---|---|
| Source | EdNet-KT1: one CSV file per student |
| Full size | 784,309 files, 5.3 GB on disk |
| Columns | `timestamp` (epoch milliseconds), `solving_id`, `question_id`, `user_answer`, `elapsed_time` |
| Student id | Not a column; taken from the file name (`u123.csv` gives `u123`) |
| Sample used | 500 students, random seed 42, **43,574 rows** |
| Sample time span | 2017-07-01 to 2019-12-02 |
| Distinct questions in the sample | 9,639 |
| Rows per student in the sample | median 10, maximum 6,332 (heavily skewed) |

EdNet-Content (`questions.csv` with 13,169 rows and columns `question_id, bundle_id, explanation_id, correct_answer, part, tags, deployed_at`, plus `lectures.csv`, `coupons.csv`, `payments.csv`) was downloaded but is **not used** by this version of the pipeline.

The raw data is not stored in this repository (`data/` is git-ignored). See [How to run](#7-how-to-run) for how to obtain it.

## 3. Architecture and data flow

```
784,309 per-user CSVs  (data/raw/kt1/KT1/)
        |  sampler_dev.ipynb: pick 500 students (seed 42), add user_id,
        |  merge, stable sort by timestamp
        v
data/sample/kt1_sample.csv          43,574 rows, globally time-ordered
        |  feeder_dev.ipynb: cut into 200-row chunks,
        |  write one file every 2 s (write to temp name, then atomic rename)
        v
data/stream_input/part-000000.csv ... part-000217.csv          218 files
        |  streaming_job_dev.ipynb: Spark readStream (CSV, explicit schema),
        |  one file per 5 s trigger
        v
Query 1  rows per micro-batch             -> console + data/output/batch_counts/
Query 2  count per 1-hour event-time      -> console + data/output/window_counts/
         window, 1-day watermark,
         update output mode
         (each query has its own checkpoint under checkpoints/)
```

| Stage | Notebook | Reads | Writes |
|---|---|---|---|
| 1. Sample | `notebooks/sampler_dev.ipynb` | `data/raw/kt1/KT1/*.csv` | `data/sample/kt1_sample.csv` |
| 2. Feed | `notebooks/feeder_dev.ipynb` | `data/sample/kt1_sample.csv` | `data/stream_input/part-*.csv` |
| 3. Process | `notebooks/streaming_job_dev.ipynb` | `data/stream_input/` | `data/output/`, `checkpoints/` |

Each notebook is organised as numbered steps with explanatory text before each step. `src/config.py` holds the shared paths, the column order and the default parameters.

## 4. Big-data concepts demonstrated

| Concept | Where in the code |
|---|---|
| Streaming file ingestion | `streaming_job_dev.ipynb`, Step 2: `readStream` on a CSV folder with `maxFilesPerTrigger` |
| Explicit schema | Step 1: `StructType` in the same column order the feeder writes |
| Atomic hand-over of files | `feeder_dev.ipynb`, `write_batch`: write to a hidden temp file, then `os.replace` |
| Event-time processing | Step 3: `event_time = to_timestamp(timestamp / 1000)`; windows use event time, not arrival time |
| Windowed aggregation | Step 4: `groupBy(window(event_time, "1 hour")).count()` |
| Watermarking | Step 4: `withWatermark("event_time", "1 day")` |
| Stateful versus stateless queries | Step 6: two separate `writeStream` queries |
| Checkpointing | Step 6: a separate `checkpointLocation` per query |
| Micro-batch triggering | Step 6: `trigger(processingTime="5 seconds")` |
| Custom sinks | Steps 5 and 5b: `foreachBatch` callbacks that write one Parquet file per batch |
| Reproducible sampling | `sampler_dev.ipynb`: fixed seed, sorted file list, stable sort |

## 5. Key design decisions

- **Sample, sort, then chunk.** Sampling 500 students keeps the run small enough for a laptop. Merging their rows and sorting globally by timestamp gives one shared timeline. The feeder keeps that order, so event times reaching Spark increase from batch to batch. This is what lets the watermark advance even though the dates span 2017 to 2019.
- **Stable sort.** Rows with identical timestamps (common across different students) keep a repeatable order between runs.
- **Positional schema.** With an explicit schema and `header=true`, Spark's CSV reader assigns columns by position and only skips the header line. The schema order must therefore equal the file column order exactly. Both derive from one constant, `KT1_STREAM_COLUMNS`, in `src/config.py`.
- **Atomic file hand-over.** Each chunk is written to a hidden temporary name and renamed into place, so Spark never lists a half-written file.
- **Two queries, not one.** The per-batch row count is stateless. The windowed count is stateful (Spark keeps partial window totals between batches), so it needs its own streaming aggregation and its own checkpoint.
- **Update output mode.** Each batch emits only the windows whose totals changed, with their running total.
- **One small Parquet file per batch, named by batch id.** Re-processing a batch overwrites its file instead of duplicating rows. The per-batch results are tiny, so they are collected with `toPandas()`.
- **Reset before start.** The reset cell clears the input folder, the checkpoints and the saved outputs together, so a run never mixes state from an earlier run.
- **Local Spark with few shuffle partitions.** `local[*]` and `spark.sql.shuffle.partitions = 4` (the default of 200 is oversized for a few hundred rows per batch).

## 6. Repository layout

```
notebooks/
  sampler_dev.ipynb          stage 1: build the sample
  feeder_dev.ipynb           stage 2: replay it as files
  streaming_job_dev.ipynb    stage 3: Spark Structured Streaming job
src/
  config.py                  paths, column order, default parameters
requirements.txt
docs/pipeline_report.md      earlier write-up of the same run
data/                        git-ignored: raw data, sample, stream_input, output
checkpoints/                 git-ignored: Spark checkpoints
```

## 7. How to run

### Prerequisites

Tested with Python 3.10.12, OpenJDK 17.0.20, pyspark 3.5.3, pandas 2.3.3, pyarrow 25.0.1 and ipykernel 7.3.0, on Ubuntu under WSL2 (Spark ran with a 1 GB driver heap). Only `pyspark` is pinned in `requirements.txt`. A JDK must be installed for Spark.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m ipykernel install --user --name pyspark-data-processing --display-name "PySpark Data Processing"
```

The notebooks use the kernel named **PySpark Data Processing**.

### Get the data

The two datasets were fetched from the shortlinks `http://bit.ly/ednet_kt1` and `http://bit.ly/ednet-content`, which redirect to Google Drive. Downloading them with a plain `wget` saves a Drive HTML page instead of a zip file (`unzip` then fails with "End-of-central-directory signature not found"), so check the result with `file`.

What worked:

```bash
mkdir -p data/raw && cd data/raw

# EdNet-Content (small file, direct link works)
curl -L "https://drive.google.com/uc?export=download&id=117aYJAWG3GU48suS66NPaB82HwFj6xWS" -o ednet_content.zip

# EdNet-KT1 (1.1 GB): Drive first shows a "can't scan for viruses" page.
# The download form on that page posts to drive.usercontent.google.com/download
# with confirm=t and a uuid value taken from the page.
curl -L "https://drive.usercontent.google.com/download?id=1AmGcOs5U31wIIqvthn9ARqJMrMTFTcaw&export=download&confirm=t&uuid=<uuid-from-the-warning-page>" -o ednet_kt1.zip

file ednet_kt1.zip ednet_content.zip        # both must say "Zip archive data"
unzip -q ednet_kt1.zip -d kt1
unzip -q ednet_content.zip -d contents
cd ../..
```

The KT1 download was verified with the `uuid` copied from the warning page; whether `confirm=t` alone is enough was not tested. The code expects the student files at `data/raw/kt1/KT1/u*.csv` (784,309 files). The content files are not read by any notebook.

### Run the pipeline

The order matters. Do not skip step 2.

1. **`sampler_dev.ipynb`**: run all cells once. The last cell writes `data/sample/kt1_sample.csv`. The earlier cells are a guided walk-through on a 5-student sample.
2. **`feeder_dev.ipynb`**: run the cells from the top down to and including the **reset cell**. It clears `data/stream_input/`, `checkpoints/` and `data/output/` (including the test file written by the `write_batch` cell). Run it **before** the queries exist.
3. **`streaming_job_dev.ipynb`**: run the cells from the top through **Step 6** (start both queries). The queries start in the background and wait for files.
4. **`feeder_dev.ipynb`**: run the **feed-loop cell**. It writes 218 files at 2 s each, about 7 minutes.
5. **`streaming_job_dev.ipynb`**: run the **Step 7** cell to watch the console output live (the Spark UI is at http://localhost:4040). It never returns by itself; interrupt it after batch 217 has printed (the whole run took about 18 minutes, see below). Then run the **cleanup cell** to stop both queries.

Rules that prevent the two failures described in [Problems encountered](#10-problems-encountered):

- **Never run the reset cell while the queries are running.** It deletes the checkpoints they are writing to and crashes them.
- If you restart the queries in the same kernel after a failure, call `spark.streams.resetTerminated()` first (the Step 7 cell already does), otherwise `awaitAnyTermination()` re-raises the old error. Restarting the kernel also works.
- If you edit a notebook outside VS Code while it is open there, close the tab without saving before reopening, or VS Code will overwrite your changes with its older copy.

### Read the results

```python
import pandas as pd

b = pd.read_parquet("data/output/batch_counts")     # batch_id, rows, processed_at
w = pd.read_parquet("data/output/window_counts")    # batch_id, window_start, window_end, events

print(len(b), b.rows.sum())                                        # batches, total rows
final = w.sort_values("batch_id").groupby("window_start").events.last()
print(len(final), final.sum())                                     # windows, sum of final counts
```

`window_counts` holds one row per window that changed in a batch, so a window appears again whenever its total grows. Its final count is its last value.

## 8. Configuration

All defaults are in `src/config.py`.

| Parameter | Default | Meaning |
|---|---|---|
| `DEFAULT_N_USERS` | 500 | Students sampled from KT1 |
| `RANDOM_SEED` | 42 | Sampling seed |
| `DEFAULT_BATCH_ROWS` | 200 | Rows per file dropped into the input folder |
| `DEFAULT_FEED_INTERVAL_SEC` | 2.0 | Pause between files written by the feeder |
| `DEFAULT_TRIGGER_INTERVAL_SEC` | 5 | Spark micro-batch trigger interval |
| `DEFAULT_MAX_FILES_PER_TRIGGER` | 1 | Files Spark reads per micro-batch |
| `DEFAULT_WINDOW_DURATION` | `"1 hour"` | Event-time window size |
| `DEFAULT_WATERMARK_DELAY` | `"1 day"` | Watermark delay on event time |

## 9. Results

These numbers come from the saved Parquet output of one complete run (`data/output/` is git-ignored, so re-run the pipeline to regenerate it). They were computed with the snippet in [Read the results](#read-the-results).

| Check | Result |
|---|---|
| Micro-batches processed | 218 (ids 0 to 217, none missing) |
| Rows per batch | 217 batches of 200 rows and one of 174 |
| Total rows processed | **43,574**, equal to the sample size |
| Reconciliation | The latest count of every window, summed over all windows, is 43,574: nothing lost, nothing counted twice |
| Distinct hourly windows | 3,117, from 2017-07-01 to 2019-12-02 (about 29 months) |
| Windows updated in more than one batch | 207 (3,324 window rows in total) |
| Windows touched per batch | 5 to 31, median 15 |
| Busiest windows | 2018-08-04 12:00 (111 events), 2018-07-05 16:00 (104), 2019-11-24 03:00 (100) |
| Wall-clock time | 18.0 minutes |
| Gap between batch completions | median 5.0 s, 95th percentile 5.1 s, maximum 5.4 s |

Observations:

- **Batches kept pace with the trigger.** The 5 s trigger was met throughout the saved run.
- **Windows are sparse.** The sample has 43,574 rows over about 29 months, so 200 consecutive rows span days, not minutes, and one batch touches windows that are days apart.
- **Window totals accumulate.** For example, the window 2018-07-25 14:00 to 15:00 showed 60 events in batch 80 and 77 in batch 81.
- **Time zone.** Spark prints window times in the machine's local time zone (UTC+3 here), three hours ahead of UTC.
- **Not verified:** that the watermark closed old windows. In update mode only changed windows are emitted, so state clean-up is not visible in this output.

## 10. Problems encountered

1. **Overlapping chunks.** The chunking loop stepped through the sample by 1 row instead of by the batch size, so the feeder wrote 43,574 files, each repeating 199 of the previous file's 200 rows. Symptom: a single hourly window (2017-07-19 15:00) grew from 6 events in batch 2 to 351 in batch 25 instead of the timeline moving on, and early counts roughly doubled between batches 0 and 1. Fix: step by the batch size, giving 218 distinct files.
2. **Checkpoints deleted under live queries.** The reset step deletes `checkpoints/`. Running it after the queries had started removed the folders the queries were writing to, and both failed with `FileNotFoundException`. Fix: reset is a separate cell that must run before the queries start, and the feed loop no longer resets anything.
3. **Stale error from `awaitAnyTermination()`.** After the crash above, restarting the queries in the same session made `awaitAnyTermination()` raise the *old* failure. Fix: `spark.streams.resetTerminated()`, or restart the kernel.
4. **Google Drive returned a web page, not a zip.** See [Get the data](#get-the-data).
5. **Positional CSV schema.** Not a failure in this run, but a known trap: a schema in the wrong column order assigns values to the wrong fields without an error. Mitigated by the shared `KT1_STREAM_COLUMNS` constant.

## 11. Limitations

- Output is written per batch as Parquet files, but there is no database, dashboard or downstream consumer; results must be read and aggregated by hand.
- The metrics are counts only. There is no per-student or per-question analysis, and the content tables are unused.
- Single machine (`local[*]`, 1 GB driver heap). This demonstrates the streaming model, not cluster-scale behaviour or fault tolerance across nodes.
- The per-batch writers collect each batch to the driver with `toPandas()`. That is fine for a few dozen rows per batch and would not scale.
- Rate mismatch: the feeder writes one file every 2 s while Spark reads one file per 5 s trigger, so Spark trails the feeder and finishes later.
- Only 500 of 784,309 students are used, and activity per student is very skewed (median 10 rows, maximum 6,332).
- Watermark-driven window closing was configured but not verified (see [Results](#9-results)).
- The pipeline is run by hand across three notebooks in a fixed order. Nothing stops someone from running the reset cell while queries are live.

## 12. Possible next steps

- Join the stream with `questions.csv` for per-question accuracy and per-part activity.
- Raise `maxFilesPerTrigger` or lower the feed interval so consumption keeps pace with the feeder.
- Add a guard that refuses to reset when checkpoints were touched recently.
- Write results with Spark sinks (or a table format) instead of `toPandas()`, and add a small dashboard on top.
- Replace the folder with a real message broker as the source, closer to the Part 1 architecture.
- Verify window finalisation, for example with append output mode, which emits a window only once the watermark has passed it.
