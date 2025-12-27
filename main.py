#!/usr/bin/env python3
"""
Data Ingestion Pipeline: GCS -> Pub/Sub -> BigQuery

This application listens to Pub/Sub messages containing GCS filenames,
downloads the JSON files, converts datetime fields to Asia/Bangkok timezone,
and inserts the data into BigQuery.
"""

import logging
import sys
from config import Config
from gcs_handler import GCSHandler
from bigquery_loader import BigQueryLoader
from pubsub_listener import PubSubListener

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class DataIngestionPipeline:
    """Main pipeline orchestrator"""

    def __init__(self):
        """Initialize pipeline components"""
        # Validate configuration
        Config.validate()

        # Initialize handlers
        self.gcs_handler = GCSHandler(
            bucket_name=Config.GCS_BUCKET_NAME,
            project_id=Config.GCP_PROJECT_ID
        )

        self.bq_loader = BigQueryLoader(
            project_id=Config.GCP_PROJECT_ID,
            dataset_id=Config.BQ_DATASET_ID,
            table_id=Config.BQ_TABLE_ID,
            schema_file=Config.BQ_SCHEMA_FILE,
            target_timezone=Config.TARGET_TIMEZONE
        )

        # Ensure BigQuery table exists
        self.bq_loader.create_table_if_not_exists()

        logger.info("Pipeline initialized successfully")

    def process_message(self, filename, message):
        """
        Process a single Pub/Sub message

        Args:
            filename: Name of the file in GCS bucket
            message: Pub/Sub message object

        Returns:
            True if processing successful, False otherwise
        """
        try:
            logger.info(f"Processing file: {filename}")

            # Download JSON file from GCS
            data = self.gcs_handler.download_json_file(filename)

            if data is None:
                logger.error(f"Failed to download file: {filename}")
                return False

            # Handle both single record and array of records
            if isinstance(data, dict):
                records = [data]
            elif isinstance(data, list):
                records = data
            else:
                logger.error(f"Unexpected data format in {filename}: {type(data)}")
                return False

            # Insert records into BigQuery
            success = self.bq_loader.insert_records(records)

            if success:
                logger.info(f"Successfully processed {len(records)} record(s) from {filename}")
            else:
                logger.error(f"Failed to insert records from {filename}")

            return success

        except Exception as e:
            logger.error(f"Error processing message for {filename}: {e}", exc_info=True)
            return False

    def run(self):
        """Start the pipeline"""
        logger.info("Starting data ingestion pipeline...")

        # Create Pub/Sub listener
        listener = PubSubListener(
            project_id=Config.GCP_PROJECT_ID,
            subscription_id=Config.PUBSUB_SUBSCRIPTION_ID,
            callback=self.process_message,
            max_messages=Config.MAX_MESSAGES,
            ack_deadline=Config.ACK_DEADLINE_SECONDS
        )

        # Start listening
        listener.listen()


def main():
    """Main entry point"""
    try:
        pipeline = DataIngestionPipeline()
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
