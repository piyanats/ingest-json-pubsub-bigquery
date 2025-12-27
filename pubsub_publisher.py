import logging
from typing import Any
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

class PubSubPublisher:
    """Handles publishing messages to Pub/Sub"""

    def __init__(self, project_id: str, topic_id: str) -> None:
        """
        Initialize Pub/Sub publisher

        Args:
            project_id: GCP project ID
            topic_id: Pub/Sub topic ID
        """
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish(self, data: str | bytes, **attributes: str) -> str | None:
        """
        Publish a message to the topic

        Args:
            data: The message data (string or bytes)
            attributes: Optional attributes to attach to the message

        Returns:
            Message ID if successful, None otherwise
        """
        try:
            encoded_data: bytes
            if isinstance(data, str):
                encoded_data = data.encode('utf-8')
            else:
                encoded_data = data
            
            future = self.publisher.publish(self.topic_path, encoded_data, **attributes)
            message_id: str = future.result()
            
            logger.info(f"Published message {message_id} to {self.topic_path}")
            return message_id
            
        except Exception as e:
            logger.error(f"Failed to publish message to {self.topic_path}: {e}")
            return None