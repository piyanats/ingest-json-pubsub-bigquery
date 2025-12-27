# CLAUDE.md - Project Context for AI Assistants

## Project Overview

This is a **data ingestion pipeline** that processes JSON files from Google Cloud Storage (GCS), triggered by Google Pub/Sub messages, and loads the data into BigQuery with automatic datetime timezone conversion.

### Workflow
1. JSON files uploaded to GCS bucket
2. GCS finalization event triggers Pub/Sub message (containing filename)
3. Python application listens to Pub/Sub subscription
4. Downloads JSON file from GCS
5. Converts all datetime fields to Asia/Bangkok timezone
6. Inserts data into BigQuery table

## Architecture Decisions

### Why This Design?

1. **Event-Driven Architecture**: Uses GCS notifications → Pub/Sub for decoupling and reliability
2. **Timezone Normalization**: All datetime fields converted to Asia/Bangkok for consistency
3. **Schema-Driven**: BigQuery schema file determines which fields need datetime conversion
4. **Modular Design**: Separate handlers for GCS, BigQuery, Pub/Sub, and datetime conversion

### Key Components

#### 1. `main.py` - Application Entry Point
- Orchestrates the entire pipeline
- Initializes all components
- Handles graceful shutdown
- Main class: `DataIngestionPipeline`

#### 2. `config.py` - Configuration Management
- Environment variable-based configuration
- Validates required settings on startup
- Default values for optional settings
- Uses `python-dotenv` for `.env` file support

#### 3. `datetime_converter.py` - DateTime Conversion
- **Critical Component**: Handles datetime conversion to Asia/Bangkok timezone
- Supports multiple input formats:
  - ISO 8601 strings
  - Various datetime string formats (via `dateutil.parser`)
  - Unix timestamps (int/float)
  - Python datetime objects
- Handles both timezone-aware and naive datetimes
- Returns naive datetime (BigQuery expects this)

#### 4. `gcs_handler.py` - GCS Operations
- Downloads JSON files from GCS bucket
- Parses JSON content
- Error handling for missing/invalid files
- Uses `google-cloud-storage` library

#### 5. `bigquery_loader.py` - BigQuery Operations
- Loads schema from `table_schema.json`
- Identifies datetime fields automatically
- Converts datetime fields before insertion
- Creates table if not exists
- Supports both single records and arrays
- Uses `google-cloud-bigquery` library

#### 6. `pubsub_listener.py` - Pub/Sub Operations
- Two modes:
  - `listen()`: Streaming pull (blocking, continuous)
  - `pull_once()`: Single pull (non-blocking, for testing)
- Message acknowledgement handling (ack/nack)
- Flow control configuration
- Uses `google-cloud-pubsub` library

## Important Schema Details

### BigQuery Schema (`table_schema.json`)
- 68 fields total
- **DateTime fields** (automatically converted to Asia/Bangkok):
  - `insertedAt`, `beforeAt`, `businessDate`, `createdAt`
  - `pointExpireDate`, `updatedAt`, `updateAt`
  - `serverTimestamp`, `docSavedAt`
- **JSON fields** (stored as JSON type):
  - `billPaymentData`, `campaign`, `coPromotionCode`, `coupon`
  - `customer`, `directDiscount`, `kBaoData`, `promotion`
  - `sellingItem`, `tenders`, and more
- All fields are NULLABLE mode

## Data Flow

```
GCS File Upload
    ↓
GCS Finalization Event
    ↓
Pub/Sub Message (filename)
    ↓
pubsub_listener.py receives message
    ↓
main.py.process_message()
    ↓
gcs_handler.py downloads JSON
    ↓
bigquery_loader.py processes data
    ↓
datetime_converter.py converts datetime fields
    ↓
BigQuery insert_rows_json()
    ↓
Message acknowledged (ack)
```

## Development Guidelines

### Adding New Features

**To add new datetime fields:**
1. Update `table_schema.json` with new field (type: DATETIME)
2. DateTime conversion is automatic - no code changes needed

**To add new data transformations:**
1. Create new transformer class (similar to `DateTimeConverter`)
2. Integrate in `bigquery_loader.py`
3. Apply transformation in `convert_record()` method

**To add new message sources:**
1. Create new listener class (similar to `PubSubListener`)
2. Update `main.py` to support multiple listeners

### Testing Considerations

**Unit Testing:**
- `datetime_converter.py`: Test various input formats and timezones
- `gcs_handler.py`: Mock GCS client, test JSON parsing
- `bigquery_loader.py`: Mock BigQuery client, test schema loading
- `pubsub_listener.py`: Mock Pub/Sub client, test callback handling

**Integration Testing:**
1. Upload test JSON to GCS
2. Verify Pub/Sub message received
3. Check BigQuery for inserted data
4. Verify datetime conversion accuracy

### Common Issues & Solutions

**Issue**: Datetime conversion fails
- **Solution**: Check input format, add logging in `datetime_converter.py`
- **Debug**: Log the raw datetime value before conversion

**Issue**: Messages not acknowledged
- **Solution**: Check callback return value in `process_message()`
- **Debug**: Ensure exceptions are caught and logged

**Issue**: BigQuery insertion fails
- **Solution**: Check schema compatibility, field types
- **Debug**: Review BigQuery error messages in logs

## Environment Variables

Required:
- `GCP_PROJECT_ID`: GCP project ID
- `PUBSUB_SUBSCRIPTION_ID`: Pub/Sub subscription name
- `GCS_BUCKET_NAME`: GCS bucket name
- `BQ_DATASET_ID`: BigQuery dataset ID
- `BQ_TABLE_ID`: BigQuery table ID

Optional:
- `TARGET_TIMEZONE`: Default: `Asia/Bangkok`
- `MAX_MESSAGES`: Default: `10`
- `ACK_DEADLINE_SECONDS`: Default: `60`
- `BQ_SCHEMA_FILE`: Default: `table_schema.json`

## GCP Permissions Required

**Service Account Permissions:**
- `roles/storage.objectViewer` - Read GCS files
- `roles/pubsub.subscriber` - Subscribe to Pub/Sub
- `roles/bigquery.dataEditor` - Insert data into BigQuery

**API Requirements:**
- Cloud Storage API
- Cloud Pub/Sub API
- BigQuery API

## Dependencies

Core libraries:
- `google-cloud-pubsub==2.23.0` - Pub/Sub client
- `google-cloud-storage==2.18.2` - GCS client
- `google-cloud-bigquery==3.25.0` - BigQuery client
- `python-dateutil==2.9.0` - Datetime parsing
- `pytz==2024.1` - Timezone handling
- `python-dotenv==1.0.1` - Environment variables

## Code Style & Patterns

**Logging:**
- Use module-level loggers: `logger = logging.getLogger(__name__)`
- Log levels: INFO for normal flow, ERROR for exceptions, WARNING for issues
- Include context in log messages (filename, record count, etc.)

**Error Handling:**
- Return `True/False` for success/failure
- Log exceptions with `exc_info=True` for stack traces
- Don't crash on single message failure - continue processing

**Configuration:**
- Centralized in `config.py`
- Validate on startup with `Config.validate()`
- Use environment variables for all settings

## Extending the Project

### Adding Support for Different File Formats

1. Create parser in `gcs_handler.py`:
```python
def download_csv_file(self, blob_name):
    # CSV parsing logic
    pass
```

2. Update `main.py` to detect format based on extension

### Adding Data Validation

1. Create `validator.py`:
```python
class DataValidator:
    def validate_record(self, record, schema):
        # Validation logic
        pass
```

2. Integrate in `bigquery_loader.py` before insertion

### Adding Metrics/Monitoring

1. Add metrics tracking:
```python
# In main.py
self.metrics = {
    'messages_processed': 0,
    'messages_failed': 0,
    'records_inserted': 0
}
```

2. Export to Cloud Monitoring or log periodically

## Future Improvements

- [ ] Add retry logic with exponential backoff for BigQuery insertion
- [ ] Add dead letter queue for failed messages
- [ ] Add batch processing for multiple files
- [ ] Add data validation before BigQuery insertion
- [ ] Add metrics/monitoring integration
- [ ] Add unit tests for all modules
- [ ] Add support for schema evolution
- [ ] Add data deduplication logic
- [ ] Add Cloud Functions deployment option
- [ ] Add Docker containerization

## Related Documentation

- [BigQuery Schema Reference](table_schema.json)
- [Setup Guide](README.md)
- [GCP Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
- [BigQuery JSON Type](https://cloud.google.com/bigquery/docs/reference/standard-sql/json-data)
