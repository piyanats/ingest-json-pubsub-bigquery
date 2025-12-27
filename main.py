#!/usr/bin/env python3
"""
Data Ingestion Pipeline: GCS -> Pub/Sub -> BigQuery

This application listens to Pub/Sub messages containing GCS filenames,
downloads the JSON files, converts datetime fields to Asia/Bangkok timezone,
and inserts the data into BigQuery.
"""

import logging
import sys
import signal
import json
from config import Config
from gcs_handler import GCSHandler
from bigquery_loader import BigQueryLoader
from pubsub_listener import PubSubListener
from pubsub_publisher import PubSubPublisher

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
        
        # Initialize Dead Letter Publisher if configured
        self.dl_publisher = None
        if Config.PUBSUB_DEAD_LETTER_TOPIC_ID:
            self.dl_publisher = PubSubPublisher(
                project_id=Config.GCP_PROJECT_ID,
                topic_id=Config.PUBSUB_DEAD_LETTER_TOPIC_ID
            )
            logger.info(f"Dead Letter Topic configured: {Config.PUBSUB_DEAD_LETTER_TOPIC_ID}")
        else:
            logger.warning("No Dead Letter Topic configured. Failed messages will be NACKed.")

        # Ensure BigQuery table exists
        self.bq_loader.create_table_if_not_exists()

        # Track listener for cleanup
        self.listener = None
        self.shutdown_requested = False

        logger.info("Pipeline initialized successfully")

    def request_shutdown(self, signum=None, frame=None):
        """Request graceful shutdown"""
        logger.info("Shutdown requested, cleaning up...")
        self.shutdown_requested = True

    def cleanup(self):
        """Cleanup resources"""
        logger.info("Cleaning up resources...")
        # Close GCS client
        if hasattr(self.gcs_handler, 'client'):
            self.gcs_handler.client.close()
        # Close BigQuery client
        if hasattr(self.bq_loader, 'client'):
            self.bq_loader.client.close()
        logger.info("Cleanup complete")

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
                # If we can't even download the file (e.g. not found), 
                # maybe we should DLT it too? For now, let's treat it as failure.
                return self._handle_failure(filename, "File download failed")

            # Handle both single record and array of records
            if isinstance(data, dict):
                records = [data]
            elif isinstance(data, list):
                records = data
            else:
                error_msg = f"Unexpected data format: {type(data)}"
                logger.error(f"{error_msg} in {filename}")
                return self._handle_failure(filename, error_msg)

            # Insert records into BigQuery
            success = self.bq_loader.insert_records(records)

            if success:
                logger.info(f"Successfully processed {len(records)} record(s) from {filename}")
                return True
            else:
                logger.error(f"Failed to insert records from {filename}")
                return self._handle_failure(filename, "BigQuery insert failed")

        except Exception as e:
            logger.error(f"Error processing message for {filename}: {e}", exc_info=True)
            return self._handle_failure(filename, str(e))

    def _handle_failure(self, filename, error_reason):
        """
        Handle processing failure by publishing to Dead Letter Topic if available.
        
        Args:
            filename: The filename related to the failure
            error_reason: Description of the error
            
        Returns:
            True if sent to DLT (so we can ACK original), False otherwise (NACK original)
        """
        if self.dl_publisher:
            logger.info(f"Publishing failure for {filename} to Dead Letter Topic...")
            
            # Prepare DLT message payload
            payload = json.dumps({
                "filename": filename,
                "error": error_reason,
                "timestamp": str(logging.Formatter.converter(None)) # rudimentary timestamp
            })
            
            # Publish to DLT
            msg_id = self.dl_publisher.publish(
                payload, 
                filename=filename, 
                error_type="ProcessingFailure"
            )
            
            if msg_id:
                logger.info("Successfully sent to DLT. Acknowledging original message to stop retry loop.")
                return True # ACK original message
            else:
                logger.error("Failed to send to DLT. NACKing original message.")
                return False # NACK original message
        else:
            # No DLT configured, just return False to NACK
            return False

    def run(self):
        """Start the pipeline"""
        logger.info("Starting data ingestion pipeline...")

        # Setup signal handlers
        signal.signal(signal.SIGINT, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)

        # Create Pub/Sub listener
        self.listener = PubSubListener(
            project_id=Config.GCP_PROJECT_ID,
            subscription_id=Config.PUBSUB_SUBSCRIPTION_ID,
            callback=self.process_message,
            max_messages=Config.MAX_MESSAGES,
            ack_deadline=Config.ACK_DEADLINE_SECONDS
        )

        try:
            # Start listening
            self.listener.listen()
        finally:
            self.cleanup()


def main():
    """Main entry point"""
    pipeline = None
    try:
        pipeline = DataIngestionPipeline()
        pipeline.run()
    except KeyboardInterrupt:
        logger.info("Pipeline stopped by user")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if pipeline:
            pipeline.cleanup()


if __name__ == '__main__':
    main()
