import json
import logging
from google.cloud import bigquery
from datetime_converter import DateTimeConverter

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Handles data loading into BigQuery"""

    def __init__(self, project_id, dataset_id, table_id, schema_file='table_schema.json', target_timezone='Asia/Bangkok'):
        """
        Initialize BigQuery loader

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            table_id: BigQuery table ID
            schema_file: Path to table schema JSON file
            target_timezone: Target timezone for datetime conversion
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.table_ref = f"{project_id}.{dataset_id}.{table_id}"
        self.datetime_converter = DateTimeConverter(target_timezone)

        # Load schema and identify datetime fields
        self.schema = self._load_schema(schema_file)
        self.datetime_fields = self._get_datetime_fields()

    def _load_schema(self, schema_file):
        """Load BigQuery schema from JSON file"""
        try:
            with open(schema_file, 'r') as f:
                schema_json = json.load(f)

            # Convert to BigQuery schema format
            schema = []
            for field in schema_json:
                bq_field = bigquery.SchemaField(
                    name=field['name'],
                    field_type=field['type'],
                    mode=field['mode']
                )
                schema.append(bq_field)

            logger.info(f"Loaded schema with {len(schema)} fields from {schema_file}")
            return schema

        except Exception as e:
            logger.error(f"Failed to load schema from {schema_file}: {e}")
            raise

    def _get_datetime_fields(self):
        """Extract datetime field names from schema"""
        datetime_fields = [
            field.name for field in self.schema
            if field.field_type in ('DATETIME', 'TIMESTAMP')
        ]
        logger.info(f"Identified {len(datetime_fields)} datetime fields: {datetime_fields}")
        return datetime_fields

    def convert_record(self, record):
        """
        Convert datetime fields in record to target timezone

        Args:
            record: Dictionary containing the data record

        Returns:
            Converted record
        """
        return self.datetime_converter.convert_record_datetimes(record, self.datetime_fields)

    def insert_record(self, record):
        """
        Insert a single record into BigQuery

        Args:
            record: Dictionary containing the data record

        Returns:
            True if successful, False otherwise
        """
        # Convert datetime fields
        converted_record = self.convert_record(record)

        return self.insert_records([converted_record])

    def insert_records(self, records):
        """
        Insert multiple records into BigQuery

        Args:
            records: List of dictionaries containing data records

        Returns:
            True if successful, False otherwise
        """
        if not records:
            logger.warning("No records to insert")
            return True

        try:
            # Convert datetime fields for all records
            converted_records = [self.convert_record(record) for record in records]

            # Insert rows
            errors = self.client.insert_rows_json(self.table_ref, converted_records)

            if errors:
                logger.error(f"Encountered errors while inserting rows: {errors}")
                return False
            else:
                logger.info(f"Successfully inserted {len(converted_records)} record(s) into {self.table_ref}")
                return True

        except Exception as e:
            logger.error(f"Failed to insert records into BigQuery: {e}")
            return False

    def create_table_if_not_exists(self):
        """Create the BigQuery table if it doesn't exist"""
        try:
            table = bigquery.Table(self.table_ref, schema=self.schema)
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Table {self.table_ref} is ready")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {self.table_ref}: {e}")
            return False
