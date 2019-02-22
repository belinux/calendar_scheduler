"""
Scheduler
"""

__author__ = "Partha Saradhi Konda<parthasaradhi1992@gmail.com>"
__version__ = 0.1

import datetime
import pytz
from croniter import croniter
from enums import TIMEZONES


def timezone_required(func):
    def wrapper(*args, **kwargs):
        timezone = kwargs.get('timezone', None)
        if timezone is None:
            raise ValueError("timezone is required")
        if timezone not in TIMEZONES:
            raise ValueError("Invalid timezone {}".format(timezone))
        return func(*args, **kwargs)
    return wrapper


def validate_cron(func):
    def wrapper(*args, **kwargs):
        cron = kwargs.get('cron', None)
        if not isinstance(cron, str):
            raise TypeError("invalid cron")
        if not croniter.is_valid(cron):
            raise ValueError("Invalid cron specified {}".format(cron))
        return func(*args, **kwargs)
    return wrapper


def valid_schedule_type(func):
    def wrapper(*args, **kwargs):
        if 'schedule_data' not in kwargs:
            raise ValueError("Invalid Schedule Type")
        schedule_type = kwargs['schedule_data'].get('schedule_type', None)
        if schedule_type is None or schedule_type.lower() not in ['date_specific', 'cron', 'recurring']:
            raise ValueError("Invalid Scheduling Type")
        return func(*args, **kwargs)
    return wrapper


class Scheduler(object):

    def _is_a_date_or_datetime(self, value):
        if not isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
            return False
        return True

    def _is_a_timezone(self, timezone):
        if timezone is None:
            return False, None
        if timezone not in TIMEZONES:
            return False, "Not in timezones list"

    def _get_schedule_eta(self, timezone=None, start_date=None, start_time=None, _format=None):
        """
        Will convert the given datetime into UTC
        If timezone is also provided, then it will localize and convert it into the UTC
        @timezone: timezone from given list
        @start_date: string or dateinstance (dd/mm/yyyy or mm/dd/yyy or mm-dd-yyy or dd-mm-yyyy)
        @start_time: string (HH:MM AM/PM/am/pm)
        @_format: dd/mm/yyyy or mm/dd/yyy or mm-dd-yyy or dd-mm-yyyy or parsable format
        """
        _start_date_time = None
        _start_time = None
        _timezone = None

        if timezone is not None:
            if timezone not in TIMEZONES:
                raise ValueError("Invalid Timezone")  # exit
            _timezone = pytz.timezone(timezone)
        if start_date is None:
            raise ValueError("start_date should not be null")  # exit
        if start_time is None:
            raise ValueError("start_time should not be null")  # exit

        # Parse time into timeobj
        try:
            _start_time = datetime.datetime.strptime(
                start_time, "%H:%M %p").time()
        except ValueError:
            raise ValueError(
                "Unable to parse the time {}".format(start_time))

        # Convert given format of date to date instance
        if not isinstance(start_date, datetime.date):
            if not isinstance(start_date, str):
                raise TypeError("Invalid type of date")  # exit
            if _format is None:
                raise ValueError("_format is not provided")  # exit
            try:
                start_date = datetime.datetime.strptime(
                    start_date, _format)
            except ValueError:
                raise ValueError("Invalid format {}".format(_format))
        _start_date_time = datetime.datetime.combine(
            start_date, _start_time
        )

        # Localize and convert to UTC
        if _timezone is not None:
            _start_date_time = _timezone.localize(
                _start_date_time)

        _start_date_time = _start_date_time.astimezone(pytz.UTC)
        return _start_date_time  # exit

    @timezone_required
    def get_next_eta_date_specific(self, timezone=None, _format=None, from_date=None, schedules=[]):
        """
        For multiple date & times
        return: eta/None, message/None
        """
        eta = None
        if not schedules:
            raise ValueError("schedule info not provided")  # exit
        if not isinstance(schedules, list):
            raise TypeError("Invalid Type of schedule, expecting `list`")

        if from_date is not None:
            if not isinstance(from_date, datetime.datetime):
                raise ValueError("Invalid datetime reference")
            current_datetime = from_date
        else:
            current_datetime = datetime.datetime.now()

        current_datetime = current_datetime.astimezone(pytz.UTC)

        for schedule in schedules:
            if not isinstance(schedule, dict):
                raise TypeError(
                    "Invalid type of schedule, expecting list of dicts")
            if 'start_date' not in schedule and 'start_time' not in schedule:
                raise ValueError(
                    "start_date & start_time are required in schedules")  # exit
            if timezone is not None:
                schedule.update({
                    'timezone': timezone})
            schedule.update({
                '_format': _format
            })
            # unpack won't cause arg error
            _eta = self._get_schedule_eta(**schedule)
            if current_datetime <= _eta:
                eta = _eta
                break  # exit
        return eta

    @timezone_required
    @validate_cron
    def get_next_eta_cron(self, timezone=None, _format=None, from_date=None, end_date=None, end_time=None, cron=None):
        """
        Which returns the next eta based on cron config
        """
        eta = None
        _end_date = None
        _end_time = None
        _end_date_time = None

        if from_date is not None:
            if not isinstance(from_date, datetime.date) or not isinstance(from_date, datetime.datetime):
                raise TypeError(
                    "expected datetime or date but got {}".format(str(type(from_date))))
        if from_date is None:
            from_date = datetime.datetime.now()

        from_date = from_date.astimezone(pytz.UTC)
        cronifier = croniter(cron, from_date)

        _eta = cronifier.get_next(datetime.datetime).astimezone(pytz.UTC)

        if end_date is not None:
            if not isinstance(end_date, datetime.datetime) or isinstance(end_date, datetime.date):
                if not isinstance(end_date, str):
                    raise TypeError("invalid date")
                if _format is None:
                    raise ValueError("_format is required")
                try:
                    _end_date = datetime.datetime.strptime(
                        end_date, _format)
                except ValueError:
                    raise ValueError("Invalid format {}".format(_format))

        if end_time is not None:
            if not isinstance(end_time, str):
                raise TypeError("invalid time")
            try:
                _end_time = datetime.datetime.strptime(
                    end_time, "%H:%M %p").time()
            except ValueError:
                raise ValueError(
                    "Unable to parse the time {}".format(end_time))

        if _end_date is not None:
            if _end_time is not None:
                _end_date_time = datetime.datetime.combine(
                    _end_date, _end_time
                )
            else:
                _end_date_time = _end_date
            _end_date_time = _end_date_time.astimezone(pytz.UTC)

        if _eta is not None and _end_date_time is not None:
            if _eta > _end_date_time:
                return None  # exit
        return _eta  # exit

    def get_next_eta_recurring(self, timezone=None, _format=None, start_date=None, start_time=None, end_date=None, end_time=None, recurring={}):
        """
        Which return the next eta based on the recurring info
        """
        pass

    @valid_schedule_type
    def get_next_eta(self, schedule_data={}):
        """
        Which accepts whole schema and returns the eta
        """
        eta = None
        schedule_type = schedule_data.get('schedule_type', None)
        timezone = schedule_data.get('timezone', None)
        schedules = schedule_data.get('schedules', [])
        cron = schedule_data.get('cron', None)
        end_date = schedule_data.get('end_date', None)
        end_time = schedule_data.get('end_time', None)
        recurring = schedule_data.get('recurring', {})
        _format = schedule_data.get('_format', '%m/%d/%Y')

        if timezone is None:
            raise ValueError("timezone is required")
        if timezone not in TIMEZONES:
            raise ValueError("Invalid timezone {}".format(timezone))

        if schedule_type.lower() == 'date_specific':
            eta = self.get_next_eta_date_specific(
                timezone=timezone, _format=_format, schedules=schedules)

        if schedule_type.lower() == 'cron':
            if not isinstance(cron, str):
                raise TypeError("invalid cron")
            if not croniter.is_valid(cron):
                raise ValueError("Invalid cron specified {}".format(cron))
            eta = self.get_next_eta_cron(
                timezone=timezone, _format=_format, end_date=end_date, end_time=end_time, cron=cron)

        if schedule_type.lower() == 'recurring':
            if not schedules:
                raise ValueError("Invalid schedules")
            if len(schedules) > 1:
                raise ValueError("schedules must be length 1")
            if not isinstance(schedules[0], dict):
                raise ValueError("Invalid scheduling info")
            start_date = schedules[0].get('start_date', None)
            start_time = schedules[0].get('start_time', None)
            if not recurring:
                raise ValueError("recurring is required")
            if not isinstance(recurring, dict):
                raise TypeError("Invalid recurring")
            eta = self.get_next_eta_recurring(
                timezone=timezone, start_date=start_date,
                start_time=start_time, end_date=end_date, end_time=end_time,
                recurring=recurring)
        return eta
