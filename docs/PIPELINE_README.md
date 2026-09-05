# YouTube Analytics Pipeline

## Overview

This project is a **medallion-architecture YouTube analytics pipeline** built on Databricks. It ingests video metadata from the YouTube Data API v3 for five target channels, transforms it through bronze, silver, and gold layers, and produces aggregated analytics tables ready for dashboarding and reporting.

The pipeline tracks key performance metrics across channels including:

* Cumulative and day-over-day view velocity
* Likes, comments, and engagement rate
* Optimal publishing windows (day-of-week and hour-of-day)
* Keyword/tag momentum analysis
* Channel-level performance summaries

---

## Architecture

```
YouTube Data API v3
        |
        v
+-------------------+     +----------------------------+
|   Bronze Layer    | --> | UC Volume (JSON landing)    |
| 01_bronze_ingestion|    | /Volumes/.../raw_landing   |
+-------------------+     +----------------------------+
        |                              |
        v                              v
+-------------------+     +----------------------------+
|   Silver Layer     | <-- | Auto Loader (cloudFiles)   |
| 02_silver_         |     | -> bronze.raw_videos table |
|  transformations   |     +----------------------------+
+-------------------+
        |
        v
+-------------------+
|   Gold Layer       |
| 03_gold_           |
|  aggregations      |
+-------------------+
        |
        v
+-------------------+
|  Dashboards / BI   |
+-------------------+
```

### Data Flow Summary

1. **Bronze** -- Raw JSON from YouTube API is landed as files in a UC Volume, then ingested into a Delta table via Auto Loader.
2. **Silver** -- Raw records are parsed, type-cast, deduplicated, and enriched with velocity/engagement metrics. Tags are normalized into a dimension table. Data quality constraints are enforced.
3. **Gold** -- Business-level aggregate tables for publishing window heatmaps, keyword momentum, channel summaries, and per-video performance.

---

## Prerequisites

### 1. Unity Catalog

The pipeline assumes the following catalog and schemas exist:

```sql
CREATE CATALOG IF NOT EXISTS youtube_lakehouse;

CREATE SCHEMA IF NOT EXISTS youtube_lakehouse.bronze;
CREATE SCHEMA IF NOT EXISTS youtube_lakehouse.silver;
CREATE SCHEMA IF NOT EXISTS youtube_lakehouse.gold;
```

### 2. UC Volume

A volume for raw JSON file landing:

```sql
CREATE VOLUME IF NOT EXISTS youtube_lakehouse.bronze.raw_landing;
```

File path: `/Volumes/youtube_lakehouse/bronze/raw_landing/videos`

Checkpoint path: `/Volumes/youtube_lakehouse/bronze/raw_landing/_checkpoints/bronze_videos`

### 3. YouTube API Key

Store your YouTube Data API v3 key in Databricks Secrets:

```python
# Run in setup_tests notebook or a one-time setup cell
dbutils.secrets.put(scope="youtube_scope", key="api_key", string_value="YOUR_API_KEY")
```

Verify retrieval:

```python
api_key = dbutils.secrets.get(scope="youtube_scope", key="api_key")
print("Secret Loaded Successfully:", len(api_key) > 0)
```

---

## Pipeline Notebooks

### Notebook 1: Bronze Ingestion (`notebooks/01_bronze_ingestion`)

**Purpose:** Fetch raw video metadata from the YouTube Data API v3 and land it as JSON files, then ingest into a Delta table.

**Key Steps:**

1. Retrieve the YouTube API key from Databricks Secrets.
2. For each of 5 target channel IDs, call the `search.list` and `videos.list` endpoints (50 videos per channel, 250 total).
3. Wrap all extracted records in a metadata envelope (`_source_batch_id`, `_extracted_at_utc`, `record_count`).
4. Write the JSON batch to a partitioned directory in the UC Volume: `year=YYYY/month=MM/youtube_batch_YYYYMMDD_HHMMSS.json`.
5. Use Auto Loader (`cloudFiles` format) to stream JSON files from the volume into `youtube_lakehouse.bronze.raw_videos` with an `AvailableNow` trigger.
6. Auto Loader config includes schema inference, schema evolution (`schemaLocation`), and a `_rescued_data` column for unparseable fields.

**Target Channels:**

| Channel ID | Description |
|---|---|
| UCf1XIplYiqNv9baGsFHwPPQ | Channel 1 |
| UC3vHW2h22WE-pNi5WJtRIjg | Channel 2 |
| UCWJPKXhkcMGXafdtqGx1mEw | Channel 3 |
| UCtm8rtofLSnaIBi3noB0INg | Channel 4 |
| UCa9qP2KdOiWsBzWe6YD2ILw | Channel 5 |

**Output Table:** `youtube_lakehouse.bronze.raw_videos`

**Schema:**

| Column | Type | Description |
|---|---|---|
| `_source_batch_id` | string | Batch identifier (timestamp-based) |
| `_extracted_at_utc` | string | ISO timestamp of extraction |
| `record_count` | long | Number of records in the batch |
| `records` | array<struct> | Array of raw YouTube video objects |
| `_ingest_timestamp` | timestamp | Auto Loader ingestion timestamp |
| `_source_file` | string | Source file path |
| `_rescued_data` | struct | Fields that didn't match the inferred schema |

---

### Notebook 2: Silver Transformations (`notebooks/02_silver_transformations`)

**Purpose:** Parse, cleanse, deduplicate, and enrich raw video data with velocity and engagement metrics.

**Key Steps:**

1. **Parse and Type Cast (Task 2.1 and 2.2):** Explode the `records` array from bronze, extract and cast fields (video_id, channel info, view/like/comment counts, publish timestamp). Deduplicate by daily snapshot.
2. **Velocity and Engagement (Task 2.3):** Compute day-over-day view delta (`delta_views_24h`) using LAG window functions partitioned by video_id. Calculate `engagement_rate_pct` as (likes + comments) / views. Flag `is_view_audit_event` for negative view deltas.
3. **Tag Normalization (Task 2.4):** Explode and normalize video tags into a dimension table (`dim_video_tags`) with lowercased, trimmed, non-empty tokens.
4. **Data Quality Audits (Task 2.5):** Validate null primary keys, negative cumulative totals, invalid engagement rates, and duplicate snapshots. All checks currently pass.
5. **Constraint Enforcement:** Add Delta `CHECK` constraints on the silver table for non-negative views, non-negative likes, and engagement rate between 0 and 100.

**Output Tables:**

`youtube_lakehouse.silver.fact_video_daily_snapshots`

| Column | Type | Description |
|---|---|---|
| `video_id` | string | YouTube video ID (primary key) |
| `channel_id` | string | Channel ID |
| `channel_title` | string | Channel display name |
| `video_title` | string | Video title |
| `raw_tags` | array<string> | Original tags from YouTube |
| `published_at` | timestamp | Video publish timestamp |
| `snapshot_date` | date | Date of the daily snapshot |
| `days_since_published` | int | Days between publish and snapshot |
| `cumulative_views` | bigint | Total views at snapshot time |
| `cumulative_likes` | bigint | Total likes at snapshot time |
| `cumulative_comments` | bigint | Total comments at snapshot time |
| `delta_views_24h` | bigint | Views gained in last 24 hours |
| `engagement_rate_pct` | double | (likes + comments) / views * 100 |
| `is_view_audit_event` | boolean | True if negative view delta detected |

`youtube_lakehouse.silver.dim_video_tags`

| Column | Type | Description |
|---|---|---|
| `video_id` | string | YouTube video ID |
| `normalized_tag` | string | Lowercased, trimmed tag token |

**Delta Constraints (on fact table):**

```sql
ALTER TABLE ... ADD CONSTRAINT valid_cumulative_views CHECK (cumulative_views >= 0);
ALTER TABLE ... ADD CONSTRAINT valid_cumulative_likes CHECK (cumulative_likes >= 0);
ALTER TABLE ... ADD CONSTRAINT valid_engagement_rate CHECK (engagement_rate_pct >= 0 AND engagement_rate_pct <= 100);
```

---

### Notebook 3: Gold Aggregations (`notebooks/03_gold_aggregations`)

**Purpose:** Create business-facing aggregate tables for analytics, reporting, and dashboarding.

**Key Steps:**

1. **Publishing Window Heatmap:** Aggregate videos by day-of-week and hour-of-day to compute total videos published, median views, median initial 48-hour velocity, and engagement rate. Produces 72 time-slot bins (7 days x 24 hours, minus empty slots).
2. **Keyword Momentum Heatmap:** Join daily snapshots with normalized tags to produce tag-level performance by publishing window.
3. **Channel Dimension:** Summarize per-channel metrics (total videos tracked, first/latest publish dates).
4. **Video Performance Fact:** Per-video performance with cumulative metrics, 48-hour initial velocity, and publishing window dimensions.
5. **Optimization:** ZORDER gold tables by commonly queried columns (publish_day_of_week, publish_hour_utc, normalized_tag, channel_title) for query acceleration.
6. **Verification Audits:** Validate that gold tables are populated, hour ranges (0-23), day-of-week (1-7), metric validity, and non-null tags. All checks pass.

**Output Tables:**

`youtube_lakehouse.gold.agg_publishing_window_heatmap`

| Column | Type | Description |
|---|---|---|
| `publish_day_name` | string | Day name (e.g., "Thursday") |
| `publish_day_of_week` | int | Day number (1=Sunday, 7=Saturday) |
| `publish_hour_utc` | int | Hour of day (0-23 UTC) |
| `total_videos_published` | long | Count of videos in this slot |
| `median_views` | long | Median cumulative views |
| `median_initial_velocity_views` | long | Median views gained in first 48 hours |
| `engagement_rate_pct` | double | Average engagement rate |

`youtube_lakehouse.gold.agg_keyword_momentum_heatmap`

| Column | Type | Description |
|---|---|---|
| `normalized_tag` | string | Normalized tag/keyword |
| `publish_day_name` | string | Day name |
| `publish_day_of_week` | int | Day number |
| `publish_hour_utc` | int | Hour of day |
| `total_videos` | long | Count of videos with this tag in this slot |
| `median_views` | long | Median cumulative views |
| `median_initial_velocity_views` | long | Median 48-hour velocity |
| `engagement_rate_pct` | double | Average engagement rate |

`youtube_lakehouse.gold.dim_channels`

| Column | Type | Description |
|---|---|---|
| `channel_id` | string | Channel ID (primary key) |
| `channel_title` | string | Channel display name |
| `total_videos_tracked` | long | Distinct videos tracked |
| `first_video_published_at` | timestamp | Earliest publish date |
| `latest_video_published_at` | timestamp | Latest publish date |

`youtube_lakehouse.gold.fact_video_performance`

| Column | Type | Description |
|---|---|---|
| `video_id` | string | Video ID (primary key) |
| `channel_id` | string | Channel ID |
| `video_title` | string | Video title |
| `published_at` | timestamp | Publish timestamp |
| `publish_day_name` | string | Day name |
| `publish_day_of_week` | int | Day number |
| `publish_hour_utc` | int | Hour of day |
| `views` | bigint | Cumulative views |
| `likes` | bigint | Cumulative likes |
| `comments` | bigint | Cumulative comments |
| `initial_velocity_views` | bigint | Views gained in first 48 hours |
| `engagement_rate_pct` | double | Engagement rate |

---

## Setup Notebook (`notebooks/setup_tests`)

**Purpose:** One-time environment setup and validation.

**Steps:**

1. Store the YouTube API key in Databricks Secrets (`youtube_scope` / `api_key`) via the Secrets REST API.
2. Verify secret retrieval.
3. Verify UC Volume landing path accessibility.

---

## Execution Order

Run the notebooks in the following sequence:

1. **setup_tests** -- One-time only (stores API key, verifies volume).
2. **01_bronze_ingestion** -- Each ingestion run (fetches latest data from YouTube API).
3. **02_silver_transformations** -- After each bronze run (rebuilds silver tables).
4. **03_gold_aggregations** -- After each silver run (rebuilds gold aggregate tables).

> **Note:** The silver and gold notebooks use `CREATE OR REPLACE TABLE`, so each run fully refreshes the target tables. Bronze uses Auto Loader with `AvailableNow` trigger for incremental file ingestion.

---

## Data Quality

The pipeline enforces data quality at multiple layers:

* **Bronze:** Schema inference with rescued data column for unparseable fields.
* **Silver:** Automated audits for null primary keys, negative cumulative totals, invalid engagement rates (0-100%), and duplicate snapshots. Delta `CHECK` constraints prevent bad data from being written.
* **Gold:** Verification audits confirm tables are populated, hour/day ranges are valid, metrics are non-negative, and tags are non-null.

All quality checks currently pass.

---

## Folder Structure

```
youtube_analytics_github/
|-- docs/
|   `-- PIPELINE_README.md       <-- This file
|-- notebooks/
|   |-- setup_tests.py            <-- One-time setup
|   |-- 01_bronze_ingestion.py    <-- API extraction + Auto Loader ingestion
|   |-- 02_silver_transformations.py  <-- Cleansing, velocity, tags, DQ
|   `-- 03_gold_aggregations.py   <-- Heatmaps, channel dim, performance fact
|-- dashboards/                   <-- Reserved for future dashboards
`-- .git/                         <-- Git version control
```

---

## Key Design Decisions

* **Auto Loader with AvailableNow trigger:** Enables incremental, batch-style file ingestion without an always-on streaming cluster.
* **UC Volume for raw landing:** Keeps raw JSON files accessible and auditable before transformation.
* **LAG window for velocity:** Day-over-day view deltas computed across daily snapshots partitioned by video.
* **View audit event flagging:** Negative view deltas are flagged rather than dropped, preserving data for investigation.
* **ZORDER optimization:** Gold tables are ZORDERed by commonly filtered columns for query performance.
* **Delta CHECK constraints:** Silver table has enforced constraints to prevent invalid data from persisting.
* **CREATE OR REPLACE TABLE in silver/gold:** Simplifies pipeline logic by fully rebuilding downstream tables each run. Suitable for current data volumes.