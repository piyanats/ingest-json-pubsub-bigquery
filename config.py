import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration management for the data ingestion pipeline"""

    # GCP Project Configuration
    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

    # Pub/Sub Configuration
    PUBSUB_SUBSCRIPTION_ID = os.getenv('PUBSUB_SUBSCRIPTION_ID')
    PUBSUB_TOPIC_ID = os.getenv('PUBSUB_TOPIC_ID', '')

    # GCS Configuration
    GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')

    # BigQuery Configuration
    BQ_DATASET_ID = os.getenv('BQ_DATASET_ID')
    BQ_TABLE_ID = os.getenv('BQ_TABLE_ID')
    BQ_SCHEMA_FILE = os.getenv('BQ_SCHEMA_FILE', 'table_schema.json')

    # Application Configuration
    TARGET_TIMEZONE = os.getenv('TARGET_TIMEZONE', 'Asia/Bangkok')
    MAX_MESSAGES = int(os.getenv('MAX_MESSAGES', '10'))
    ACK_DEADLINE_SECONDS = int(os.getenv('ACK_DEADLINE_SECONDS', '60'))

    @classmethod
    def validate(cls):
        """Validate that all required configuration is present"""
        required_vars = [
            'GCP_PROJECT_ID',
            'PUBSUB_SUBSCRIPTION_ID',
            'GCS_BUCKET_NAME',
            'BQ_DATASET_ID',
            'BQ_TABLE_ID'
        ]

        missing = [var for var in required_vars if not getattr(cls, var)]

        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")

        return True
