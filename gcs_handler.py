import json
import logging
from google.cloud import storage

logger = logging.getLogger(__name__)


class GCSHandler:
    """Handles interactions with Google Cloud Storage"""

    # Default max file size: 100MB
    DEFAULT_MAX_FILE_SIZE_MB = 100

    def __init__(self, bucket_name, project_id=None, max_file_size_mb=None):
        """
        Initialize GCS handler

        Args:
            bucket_name: Name of the GCS bucket
            project_id: GCP project ID (optional)
            max_file_size_mb: Maximum file size in MB (default: 100MB)
        """
        self.bucket_name = bucket_name
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)
        self.max_file_size_bytes = (max_file_size_mb or self.DEFAULT_MAX_FILE_SIZE_MB) * 1024 * 1024

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

            # Reload blob metadata to get size
            blob.reload()

            # Check file size
            if blob.size > self.max_file_size_bytes:
                max_size_mb = self.max_file_size_bytes / (1024 * 1024)
                actual_size_mb = blob.size / (1024 * 1024)
                logger.error(f"File {blob_name} is too large ({actual_size_mb:.2f}MB). Maximum allowed: {max_size_mb:.2f}MB")
                return None

            # Download as string with explicit encoding and parse JSON
            content = blob.download_as_text(encoding='utf-8')
            data = json.loads(content)

            logger.info(f"Successfully downloaded and parsed {blob_name} ({blob.size} bytes)")
            return data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON from {blob_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to download {blob_name} from GCS: {e}", exc_info=True)
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
