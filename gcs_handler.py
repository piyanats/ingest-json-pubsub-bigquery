import json
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSHandler:
    """Handles interactions with Google Cloud Storage"""

    def __init__(self, bucket_name, project_id=None):
        """
        Initialize GCS handler

        Args:
            bucket_name: Name of the GCS bucket
            project_id: GCP project ID (optional)
        """
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def download_json_file(self, blob_name):
        """
        Download and parse JSON file from GCS

        Args:
            blob_name: Name/path of the file in GCS bucket

        Returns:
            Parsed JSON data as Python object
        """
        try:
            blob = self.bucket.blob(blob_name)

            if not blob.exists():
                logger.error(f"File {blob_name} does not exist in bucket {self.bucket_name}")
                return None

            # Download as string and parse JSON
            content = blob.download_as_text()
            data = json.loads(content)

            logger.info(f"Successfully downloaded and parsed {blob_name}")
            return data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from {blob_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to download {blob_name} from GCS: {e}")
            return None

    def list_files(self, prefix=None):
        """
        List files in the bucket

        Args:
            prefix: Optional prefix to filter files

        Returns:
            List of blob names
        """
        try:
            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list files in bucket {self.bucket_name}: {e}")
            return []
