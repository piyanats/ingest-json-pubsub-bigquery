from datetime import datetime
from dateutil import parser
import pytz
import logging

logger = logging.getLogger(__name__)


class DateTimeConverter:
    """Handles datetime conversion from various formats and timezones to target timezone"""

    def __init__(self, target_timezone='Asia/Bangkok'):
        self.target_tz = pytz.timezone(target_timezone)

    def convert_to_target_timezone(self, datetime_value):
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
            # Handle datetime object
            if isinstance(datetime_value, datetime):
                dt = datetime_value
            # Handle string datetime
            elif isinstance(datetime_value, str):
                dt = parser.parse(datetime_value)
            # Handle timestamp (int or float)
            elif isinstance(datetime_value, (int, float)):
                dt = datetime.fromtimestamp(datetime_value, tz=pytz.UTC)
            else:
                logger.warning(f"Unsupported datetime type: {type(datetime_value)}")
                return None

            # If datetime is naive (no timezone), assume UTC
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)

            # Convert to target timezone
            target_dt = dt.astimezone(self.target_tz)

            # Return as naive datetime (BigQuery expects naive datetimes)
            return target_dt.replace(tzinfo=None)

        except (ValueError, TypeError, parser.ParserError) as e:
            logger.error(f"Failed to convert datetime '{datetime_value}': {e}")
            return None

    def convert_record_datetimes(self, record, datetime_fields):
        """
        Convert all datetime fields in a record to target timezone

        Args:
            record: Dictionary containing the data record
            datetime_fields: List of field names that contain datetime values

        Returns:
            Modified record with converted datetime fields
        """
        converted_record = record.copy()

        for field in datetime_fields:
            if field in converted_record:
                converted_value = self.convert_to_target_timezone(converted_record[field])
                converted_record[field] = converted_value

        return converted_record
