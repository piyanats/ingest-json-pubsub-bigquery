# Ingest JSON PubSub BigQuery

## Project Overview
This project is a data ingestion pipeline designed to automatically process JSON files uploaded to Google Cloud Storage (GCS). It listens for file upload notifications via Google Cloud Pub/Sub, downloads the files, normalizes all datetime fields to the `Asia/Bangkok` timezone, and loads the data into Google BigQuery.

**Key Optimization Features:**
*   **Cost Optimized:** Uses BigQuery **Batch Load Jobs** (`load_table_from_json`) instead of streaming inserts.
*   **Memory Optimized:** Implements **in-place data modification** (`in_place=True`) to minimize RAM usage during processing.
*   **Resilient:** Implements **Dead Letter Topic (DLT)** routing for failed messages to prevent infinite retry loops.
*   **Modern Python:** Utilizes Python 3.12+ features (Type Hinting, `zoneinfo`).

## Architecture

```mermaid
graph LR
    GCS[GCS Bucket] -->|Notification| PubSub[Pub/Sub Main]
    PubSub -->|Message| App[Python App]
    App -->|Download| GCS
    App -->|Convert TZ (In-Place)| App
    
    subgraph Success
    App -->|Batch Load| BQ[BigQuery]
    end
    
    subgraph Failure
    App -->|Publish Error| DLT[Pub/Sub DLT]
    end
```

## Key Files & Modules

*   **`main.py`**: The application entry point. Orchestrates the flow, handles errors, and routes failed messages to the Dead Letter Topic.
*   **`bigquery_loader.py`**: 
    *   Uses `client.load_table_from_json` (Batch Load).
    *   Iterates through records using **in-place** modification.
*   **`pubsub_publisher.py`**: Handles publishing failure events to the Dead Letter Topic.
*   **`datetime_converter.py`**: Utility for parsing and converting datetimes with `in_place=True` support. Uses `zoneinfo`.
*   **`pubsub_listener.py`**: Manages the subscription connection.
*   **`gcs_handler.py`**: Downloads JSON files from GCS.
*   **`config.py`**: Configuration management.
*   **`setup_env.sh`**: Automated environment setup script using `uv`.

## Setup & Usage

### Prerequisites
*   **Python 3.12+**
*   Google Cloud Credentials (ADC)
*   `.env` file configured

### Installation
Recommended method using `setup_env.sh`:
```bash
./setup_env.sh
source .venv/bin/activate
```

### Configuration (`.env`)
Ensure `MAX_MESSAGES` is set conservatively (e.g., `1` or `2`) if processing large files.

```ini
MAX_MESSAGES=1
ACK_DEADLINE_SECONDS=60
TARGET_TIMEZONE=Asia/Bangkok
PUBSUB_DEAD_LETTER_TOPIC_ID=your-dlt-topic-id
```

### Running the Pipeline
```bash
python main.py
```

## Development Conventions

1.  **Memory Management**:
    *   **Strictly avoid deep copying** large lists or dictionaries.
    *   Always use `in_place=True` when modifying data structures.
    *   Keep `MAX_MESSAGES` low to control concurrency.

2.  **Error Handling**:
    *   If a process fails (download, parse, insert), catch the exception.
    *   Publish details to the **Dead Letter Topic** (if configured).
    *   **ACK** the original message to clear the queue.
    *   Only **NACK** if the DLT publish fails or is not configured.

3.  **BigQuery Loading**:
    *   Prefer **Load Jobs** over Streaming Inserts for cost efficiency.
