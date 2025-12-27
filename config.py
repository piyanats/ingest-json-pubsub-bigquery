import os
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration management for the data ingestion pipeline"""

    # GCP Project Configuration
    GCP_PROJECT_ID: str | None = os.getenv('GCP_PROJECT_ID')

    # Pub/Sub Configuration
    PUBSUB_SUBSCRIPTION_ID: str | None = os.getenv('PUBSUB_SUBSCRIPTION_ID')
    PUBSUB_TOPIC_ID: str = os.getenv('PUBSUB_TOPIC_ID', '')
    PUBSUB_DEAD_LETTER_TOPIC_ID: str | None = os.getenv('PUBSUB_DEAD_LETTER_TOPIC_ID')

    # GCS Configuration
    GCS_BUCKET_NAME: str | None = os.getenv('GCS_BUCKET_NAME')

    # BigQuery Configuration
    BQ_DATASET_ID: str | None = os.getenv('BQ_DATASET_ID')
    BQ_TABLE_ID: str | None = os.getenv('BQ_TABLE_ID')
    BQ_SCHEMA_FILE: str = os.getenv('BQ_SCHEMA_FILE', 'table_schema.json')

    # Application Configuration
    TARGET_TIMEZONE: str = os.getenv('TARGET_TIMEZONE', 'Asia/Bangkok')

    # Parse integer values with error handling
    try:
        MAX_MESSAGES: int = int(os.getenv('MAX_MESSAGES', '10'))
        if MAX_MESSAGES <= 0:
            raise ValueError("MAX_MESSAGES must be positive")
    except ValueError as e:
        raise ValueError(f"Invalid MAX_MESSAGES value: {e}")

    try:
        ACK_DEADLINE_SECONDS: int = int(os.getenv('ACK_DEADLINE_SECONDS', '60'))
        if ACK_DEADLINE_SECONDS <= 0:
            raise ValueError("ACK_DEADLINE_SECONDS must be positive")
    except ValueError as e:
        raise ValueError(f"Invalid ACK_DEADLINE_SECONDS value: {e}")

    @classmethod
    def validate(cls) -> bool:
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

        # Validate timezone
        try:
            ZoneInfo(cls.TARGET_TIMEZONE)
        except ZoneInfoNotFoundError:
            raise ValueError(f"Invalid timezone: {cls.TARGET_TIMEZONE}")

        return True