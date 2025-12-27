import json
import logging
from typing import Callable, Any
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

logger = logging.getLogger(__name__)


class PubSubListener:
    """Handles Pub/Sub message listening and processing"""

    def __init__(
        self, 
        project_id: str, 
        subscription_id: str, 
        callback: Callable[[str, Any], bool], 
        max_messages: int = 10, 
        ack_deadline: int = 60
    ) -> None:
        """
        Initialize Pub/Sub listener

        Args:
            project_id: GCP project ID
            subscription_id: Pub/Sub subscription ID
            callback: Function to call when message is received
            max_messages: Maximum number of messages to pull at once
            ack_deadline: Acknowledgement deadline in seconds
        """
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.callback = callback
        self.max_messages = max_messages
        self.ack_deadline = ack_deadline

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

    def _parse_gcs_notification(self, message_data: bytes) -> str | None:
        """
        Parse GCS notification message to extract filename

        Args:
            message_data: Raw message data bytes

        Returns:
            Filename from GCS notification, or the decoded string if not JSON
        """
        try:
            decoded = message_data.decode('utf-8')

            # Try to parse as JSON (GCS notification format)
            try:
                notification = json.loads(decoded)
                # GCS notifications have 'name' field with the object name
                if 'name' in notification:
                    return notification['name']
            except json.JSONDecodeError:
                # Not JSON, treat as plain filename
                pass

            # Fallback to treating the entire message as filename
            return decoded

        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode message data: {e}")
            return None

    def _message_callback(self, message: Any) -> None:
        """
        Internal callback to handle incoming messages

        Args:
            message: Pub/Sub message object
        """
        try:
            # Parse GCS notification or plain filename
            filename = self._parse_gcs_notification(message.data)

            if filename is None:
                logger.error("Failed to parse filename from message")
                message.nack()
                return

            logger.info(f"Received message: {filename}")

            # Call the user-provided callback
            success = self.callback(filename, message)

            # Acknowledge the message if successful
            if success:
                message.ack()
                logger.info(f"Message acknowledged: {filename}")
            else:
                message.nack()
                logger.warning(f"Message nacked: {filename}")

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            message.nack()

    def listen(self) -> None:
        """
        Start listening for Pub/Sub messages (blocking)

        This will run indefinitely until interrupted
        """
        # Configure flow control
        flow_control = pubsub_v1.types.FlowControl(max_messages=self.max_messages)

        logger.info(f"Starting to listen on subscription: {self.subscription_path}")

        streaming_pull_future = self.subscriber.subscribe(
            self.subscription_path,
            callback=self._message_callback,
            flow_control=flow_control
        )

        try:
            # Block and wait for messages
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down...")
            streaming_pull_future.cancel()

    def pull_once(self, max_messages: int | None = None) -> int:
        """
        Pull messages once (non-blocking)

        Args:
            max_messages: Number of messages to pull (defaults to self.max_messages)

        Returns:
            Number of messages processed
        """
        if max_messages is None:
            max_messages = self.max_messages

        try:
            # Pull messages
            response = self.subscriber.pull(
                request={
                    "subscription": self.subscription_path,
                    "max_messages": max_messages
                },
                timeout=10
            )

            ack_ids = []
            processed = 0

            for received_message in response.received_messages:
                try:
                    # Parse GCS notification or plain filename
                    filename = self._parse_gcs_notification(received_message.message.data)

                    if filename is None:
                        logger.error("Failed to parse filename from message")
                        continue

                    logger.info(f"Received message: {filename}")

                    # Call the user-provided callback
                    success = self.callback(filename, received_message.message)

                    if success:
                        ack_ids.append(received_message.ack_id)
                        processed += 1
                    else:
                        logger.warning(f"Callback failed for message: {filename}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)

            # Acknowledge successful messages
            if ack_ids:
                self.subscriber.acknowledge(
                    request={
                        "subscription": self.subscription_path,
                        "ack_ids": ack_ids
                    }
                )

            logger.info(f"Processed {processed} message(s)")
            return processed

        except TimeoutError:
            logger.info("No messages available")
            return 0
        except Exception as e:
            logger.error(f"Error pulling messages: {e}")
            return 0