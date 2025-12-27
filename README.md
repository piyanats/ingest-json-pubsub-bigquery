# GCS to BigQuery Data Ingestion Pipeline

A cost-optimized and memory-efficient data ingestion pipeline that automatically processes JSON files from Google Cloud Storage (GCS) and loads them into BigQuery with automatic datetime timezone conversion.

## Architecture

```
GCS Bucket (JSON files)
    ↓ (finalization event)
Cloud Pub/Sub (filename messages)
    ↓ (subscription)
Python Application (this project)
    │
    ├── [Success] ──→ BigQuery Table (Batch Load Job)
    │
    └── [Failure] ──→ Dead Letter Topic (Pub/Sub)
```

## Key Features

- **Cost Optimized**: Uses BigQuery **Load Jobs** (Batch) instead of Streaming Inserts to minimize costs (Free tier friendly).
- **Memory Efficient**: Implements **in-place** data modification to handle large files (up to ~100MB) with minimal RAM usage.
- **Dead Letter Handling**: Automatically routes failed messages to a Dead Letter Topic for later analysis, preventing infinite retry loops.
- **Automatic Datetime Conversion**: Normalizes all datetime fields to `Asia/Bangkok` timezone.
- **Schema-Driven**: Dynamically identifies datetime fields using the BigQuery schema file.
- **Modern Python**: Built with Python 3.12+ features for better performance and type safety.

## Prerequisites

- **Python 3.12** or higher
- Google Cloud Platform account with enabled APIs:
  - Cloud Storage
  - Cloud Pub/Sub
  - BigQuery
- Service Account with permissions:
  - `roles/storage.objectViewer`
  - `roles/pubsub.subscriber`
  - `roles/pubsub.publisher` (for Dead Letter Topic)
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`

## Installation

### Automated Setup (Recommended)
We provide a script to automatically set up the environment using `uv` (a fast Python package installer).

```bash
./setup_env.sh
```
This will install `uv`, create a virtual environment in `.venv`, and install all dependencies.

### Manual Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd ingest-json-pubsub-bigquery
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

Edit `.env` file:

```bash
# GCP Project Configuration
GCP_PROJECT_ID=your-gcp-project-id

# Pub/Sub Configuration
PUBSUB_SUBSCRIPTION_ID=your-subscription-id
PUBSUB_TOPIC_ID=your-topic-id
PUBSUB_DEAD_LETTER_TOPIC_ID=your-dead-letter-topic-id  # Optional: For failed messages

# GCS Configuration
GCS_BUCKET_NAME=your-bucket-name

# BigQuery Configuration
BQ_DATASET_ID=your-dataset-id
BQ_TABLE_ID=your-table-id
BQ_SCHEMA_FILE=table_schema.json

# Application Configuration
TARGET_TIMEZONE=Asia/Bangkok
MAX_MESSAGES=1  # Recommended: Keep low (1-2) for large files to save memory
ACK_DEADLINE_SECONDS=60
```

## Usage

1. Activate the environment (if not already active):
   ```bash
   # If using setup_env.sh
   source .venv/bin/activate
   
   # If using manual setup
   source venv/bin/activate
   ```

2. Run the pipeline:
   ```bash
   python main.py
   ```

### How it works:
1.  **Listen**: Waits for Pub/Sub messages triggering on GCS file upload.
2.  **Download**: Stream downloads the JSON file.
3.  **Process**: Converts datetime fields in-place (no memory copy).
4.  **Load**: Submits a BigQuery Load Job.
5.  **Handle Result**:
    *   **Success**: Acknowledges the Pub/Sub message.
    *   **Failure**: Publishes error details to the Dead Letter Topic and Acknowledges the original message (to stop retry).
    *   **Config Error/No DLT**: NACKs the message to allow standard Pub/Sub retries.

## Error Handling & Dead Letter Topic

If a file fails to process (e.g., invalid JSON, schema mismatch, BigQuery error):
1. The error is logged.
2. A JSON payload with `filename` and `error` details is published to the `PUBSUB_DEAD_LETTER_TOPIC_ID`.
3. The original message is Acknowledged (removed from the main queue).

If `PUBSUB_DEAD_LETTER_TOPIC_ID` is not configured, failed messages are Negative Acknowledged (NACK), causing Pub/Sub to retry delivery.

## Project Structure

```
.
├── main.py                 # Orchestrator & Error Handling
├── bigquery_loader.py     # Batch Load Job logic (Memory Optimized)
├── datetime_converter.py  # Timezone conversion (In-Place)
├── gcs_handler.py         # File download
├── pubsub_listener.py     # Message consumer
├── pubsub_publisher.py    # Dead Letter publisher
├── config.py              # Config validation
├── setup_env.sh           # Environment setup script
├── table_schema.json      # BigQuery Schema
└── requirements.txt       # Dependencies
```

## License

See LICENSE file for details.
