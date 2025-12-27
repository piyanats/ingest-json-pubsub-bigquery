from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dateutil import parser
import logging
import copy
from typing import Any

logger = logging.getLogger(__name__)


class DateTimeConverter:
    """Handles datetime conversion from various formats and timezones to target timezone"""

    def __init__(self, target_timezone: str = 'Asia/Bangkok') -> None:
        self.target_tz = ZoneInfo(target_timezone)

    def convert_to_target_timezone(self, datetime_value: str | datetime | int | float | None) -> datetime | None:
        """
        Convert datetime from any format/timezone to target timezone

        Args:
            datetime_value: Can be string, datetime object, or timestamp

        Returns:
            datetime object in target timezone or None if conversion fails
        """
        if datetime_value is None or datetime_value == '':
            return None

        try:
            dt: datetime
            
            # Handle datetime object
            if isinstance(datetime_value, datetime):
                dt = datetime_value
            # Handle string datetime
            elif isinstance(datetime_value, str):
                dt = parser.parse(datetime_value)
            # Handle timestamp (int or float)
            elif isinstance(datetime_value, (int, float)):
                # Check if timestamp is in milliseconds (common in JavaScript)
                # Timestamps after year 3000 are likely in milliseconds
                if datetime_value > 32_503_680_000:  # Jan 1, 3000 in seconds
                    dt = datetime.fromtimestamp(datetime_value / 1000, tz=timezone.utc)
                else:
                    dt = datetime.fromtimestamp(datetime_value, tz=timezone.utc)
            else:
                logger.warning(f"Unsupported datetime type: {type(datetime_value)}")
                return None

            # If datetime is naive (no timezone), assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

            # Convert to target timezone
            target_dt = dt.astimezone(self.target_tz)

            # Return as naive datetime (BigQuery expects naive datetimes)
            return target_dt.replace(tzinfo=None)

        except (ValueError, TypeError, parser.ParserError) as e:
            logger.error(f"Failed to convert datetime '{datetime_value}': {e}")
            return None

    def convert_record_datetimes(
        self, 
        record: dict[str, Any], 
        datetime_fields: list[str], 
        in_place: bool = False
    ) -> dict[str, Any]:
        """
        Convert all datetime fields in a record to target timezone

        Args:
            record: Dictionary containing the data record
            datetime_fields: List of field names that contain datetime values
            in_place: If True, modifies the record directly. If False, creates a deep copy.

        Returns:
            Modified record with converted datetime fields
        """
        target_record: dict[str, Any]
        
        if in_place:
            target_record = record
        else:
            # Use deep copy to avoid modifying nested structures
            target_record = copy.deepcopy(record)

        for field in datetime_fields:
            if field in target_record and target_record[field] is not None:
                converted_value = self.convert_to_target_timezone(target_record[field])
                target_record[field] = converted_value

        return target_record