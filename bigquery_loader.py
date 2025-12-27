import json
import logging
import time
from typing import Any
from google.cloud import bigquery
from google.api_core import exceptions as google_exceptions
from datetime_converter import DateTimeConverter

logger = logging.getLogger(__name__)


class BigQueryLoader:
    """Handles data loading into BigQuery"""

    def __init__(
        self, 
        project_id: str, 
        dataset_id: str, 
        table_id: str, 
        schema_file: str = 'table_schema.json', 
        target_timezone: str = 'Asia/Bangkok'
    ) -> None:
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
        self.schema: list[bigquery.SchemaField] = self._load_schema(schema_file)
        self.datetime_fields: list[str] = self._get_datetime_fields()

    def _load_schema(self, schema_file: str) -> list[bigquery.SchemaField]:
        """Load BigQuery schema from JSON file"""
        try:
            with open(schema_file, 'r') as f:
                schema_json = json.load(f)

            # Convert to BigQuery schema format
            schema: list[bigquery.SchemaField] = []
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

    def _get_datetime_fields(self) -> list[str]:
        """Extract datetime field names from schema"""
        datetime_fields = [
            field.name for field in self.schema
            if field.field_type in ('DATETIME', 'TIMESTAMP')
        ]
        logger.info(f"Identified {len(datetime_fields)} datetime fields: {datetime_fields}")
        return datetime_fields

    def convert_record(self, record: dict[str, Any], in_place: bool = False) -> dict[str, Any]:
        """
        Convert datetime fields in record to target timezone

        Args:
            record: Dictionary containing the data record
            in_place: If True, modifies the record directly.

        Returns:
            Converted record
        """
        return self.datetime_converter.convert_record_datetimes(record, self.datetime_fields, in_place=in_place)

    def insert_record(self, record: dict[str, Any]) -> bool:
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

    def insert_records(self, records: list[dict[str, Any]], max_retries: int = 3) -> bool:
        """
        Insert multiple records into BigQuery using Load Job (Batch Insert) for cost optimization.
        
        Args:
            records: List of dictionaries containing data records
            max_retries: Maximum number of retry attempts (Not used for Load Jobs in the same way, kept for interface compatibility)

        Returns:
            True if successful, False otherwise
        """
        if not records:
            logger.warning("No records to insert")
            return True

        # Convert datetime fields for all records IN-PLACE to save memory
        for record in records:
            self.convert_record(record, in_place=True)

        job_config = bigquery.LoadJobConfig(
            schema=self.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        try:
            # Create a Load Job (Batch Insert)
            # This is generally free compared to streaming inserts
            job = self.client.load_table_from_json(
                records,  # Use the modified records directly
                self.table_ref,
                job_config=job_config
            )

            # Wait for the job to complete
            job.result()

            logger.info(f"Successfully loaded {len(records)} record(s) into {self.table_ref}")
            return True

        except Exception as e:
            logger.error(f"Failed to load records into BigQuery: {e}", exc_info=True)
            # Check for specific job errors
            if hasattr(e, 'errors') and e.errors:
                logger.error(f"Job errors: {e.errors}")
            return False

    def create_table_if_not_exists(self) -> bool:
        """Create the BigQuery table if it doesn't exist"""
        try:
            table = bigquery.Table(self.table_ref, schema=self.schema)
            table = self.client.create_table(table, exists_ok=True)
            logger.info(f"Table {self.table_ref} is ready")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {self.table_ref}: {e}")
            return False