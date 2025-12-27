# GCS to BigQuery Data Ingestion Pipeline

A data ingestion pipeline that automatically processes JSON files from Google Cloud Storage (GCS) and loads them into BigQuery with automatic datetime timezone conversion.

## Architecture

```
GCS Bucket (JSON files)
    ↓ (finalization event)
Cloud Pub/Sub (filename messages)
    ↓ (subscription)
Python Application (this project)
    ↓ (download & process)
BigQuery Table
```

## Workflow

1. JSON files are uploaded to a GCS bucket
2. When a file is finalized, a Pub/Sub message is triggered containing the filename
3. This application listens to the Pub/Sub subscription
4. Upon receiving a message, it:
   - Downloads the JSON file from GCS
   - Converts all datetime fields to Asia/Bangkok timezone
   - Inserts the data into BigQuery
5. Acknowledges the message upon successful processing

## Features

- **Automatic datetime conversion**: Handles multiple datetime formats and timezones
- **Timezone normalization**: Converts all datetime fields to Asia/Bangkok timezone
- **Flexible JSON handling**: Supports both single records and arrays of records
- **Error handling**: Proper error handling and message acknowledgement
- **Schema-driven**: Uses BigQuery schema file to identify datetime fields
- **Scalable**: Configurable message processing and flow control

## Prerequisites

- Python 3.8 or higher
- Google Cloud Platform account
- GCP project with the following APIs enabled:
  - Cloud Storage API
  - Cloud Pub/Sub API
  - BigQuery API
- Service account with appropriate permissions:
  - `roles/storage.objectViewer` for GCS bucket
  - `roles/pubsub.subscriber` for Pub/Sub subscription
  - `roles/bigquery.dataEditor` for BigQuery table

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd ingest-json-pubsub-bigquery
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up GCP authentication:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

5. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

Edit the `.env` file with your GCP project settings:

```bash
# GCP Project Configuration
GCP_PROJECT_ID=your-gcp-project-id

# Pub/Sub Configuration
PUBSUB_SUBSCRIPTION_ID=your-subscription-id
PUBSUB_TOPIC_ID=your-topic-id

# GCS Configuration
GCS_BUCKET_NAME=your-bucket-name

# BigQuery Configuration
BQ_DATASET_ID=your-dataset-id
BQ_TABLE_ID=your-table-id
BQ_SCHEMA_FILE=table_schema.json

# Application Configuration
TARGET_TIMEZONE=Asia/Bangkok
MAX_MESSAGES=10
ACK_DEADLINE_SECONDS=60
```

## GCP Setup

### 1. Create GCS Bucket
```bash
gsutil mb -p your-project-id gs://your-bucket-name
```

### 2. Create Pub/Sub Topic and Subscription
```bash
# Create topic
gcloud pubsub topics create your-topic-id --project=your-project-id

# Create subscription
gcloud pubsub subscriptions create your-subscription-id \
  --topic=your-topic-id \
  --ack-deadline=60 \
  --project=your-project-id
```

### 3. Configure GCS to Pub/Sub Notification
```bash
gsutil notification create \
  -t your-topic-id \
  -f json \
  -e OBJECT_FINALIZE \
  gs://your-bucket-name
```

### 4. Create BigQuery Dataset and Table
```bash
# Create dataset
bq mk --dataset your-project-id:your-dataset-id

# Create table (the application will do this automatically, or you can do it manually)
bq mk --table \
  your-project-id:your-dataset-id.your-table-id \
  table_schema.json
```

## Usage

### Run the pipeline:
```bash
python main.py
```

The application will:
- Connect to the Pub/Sub subscription
- Listen for messages continuously
- Process each message as it arrives
- Log all activities to stdout

### Stop the pipeline:
Press `Ctrl+C` to gracefully shut down the application.

## Project Structure

```
.
├── main.py                 # Main application entry point
├── config.py              # Configuration management
├── datetime_converter.py  # Datetime conversion utility
├── gcs_handler.py         # GCS file operations
├── bigquery_loader.py     # BigQuery data loading
├── pubsub_listener.py     # Pub/Sub message listener
├── requirements.txt       # Python dependencies
├── table_schema.json      # BigQuery table schema
├── .env.example          # Example environment variables
└── README.md             # This file
```

## DateTime Conversion

The application automatically converts datetime fields from various formats and timezones to `Asia/Bangkok` timezone. Supported input formats:

- ISO 8601 format: `2024-01-15T10:30:00Z`
- Various string formats (using dateutil parser)
- Unix timestamps (seconds since epoch)
- Datetime objects

### DateTime Fields in Schema

The following fields are automatically converted (as defined in `table_schema.json`):
- insertedAt
- beforeAt
- businessDate
- createdAt
- pointExpireDate
- updatedAt
- updateAt
- serverTimestamp
- docSavedAt

## Error Handling

- Failed message processing will result in message NACK (not acknowledged)
- The message will be redelivered according to Pub/Sub retry policy
- All errors are logged with full stack traces
- Invalid JSON files are logged but won't crash the application

## Monitoring

Monitor the application using:

1. **Application logs**: Check stdout for processing logs
2. **Pub/Sub metrics**: Monitor subscription metrics in GCP Console
3. **BigQuery**: Query the table to verify data insertion
4. **GCS bucket**: Check notification configuration

## Testing

### Test the pipeline:

1. Upload a test JSON file to GCS:
```bash
gsutil cp test-data.json gs://your-bucket-name/test-data.json
```

2. Check application logs for processing
3. Query BigQuery to verify data:
```bash
bq query --use_legacy_sql=false \
  'SELECT * FROM `your-project-id.your-dataset-id.your-table-id` LIMIT 10'
```

## Troubleshooting

### Messages not being received
- Verify GCS notification is configured correctly
- Check Pub/Sub subscription exists and is active
- Ensure service account has proper permissions

### Data not appearing in BigQuery
- Check application logs for errors
- Verify BigQuery table exists
- Check service account has BigQuery write permissions

### Datetime conversion issues
- Check input datetime format in source files
- Verify timezone configuration in `.env`
- Review conversion logs for specific errors

## License

See LICENSE file for details.
