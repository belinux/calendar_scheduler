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

    def _combine_date_time(self, timezone=None, _format=None, start_date=None, start_time=None):
        _start_date = None
        _start_time = None
        _start_date_time = None
        _timezone = None

        if start_date is None:
            return None

        if timezone is not None:
            if not isinstance(timezone, str):
                raise ValueError("invalid timezone")
            _timezone = pytz.timezone(timezone)
        if start_time is not None:
            if not isinstance(start_time, datetime.time):
                if not isinstance(start_time, str):
                    raise ValueError(
                        "Unable to parse the time {}".format(start_time))
                try:
                    _start_time = datetime.datetime.strptime(
                        start_time, "%H:%M %p").time()
                except ValueError:
                    raise ValueError(
                        "Unable to parse the time {}".format(start_time))

        if not isinstance(start_date, datetime.date):
            if not isinstance(start_date, str):
                raise TypeError("Invalid type of date")  # exit
            if _format is None:
                raise ValueError("_format is not provided")  # exit
            try:
                _start_date = datetime.datetime.strptime(
                    start_date, _format)
            except ValueError:
                raise ValueError("Invalid format {}".format(_format))
        else:
            _start_date = start_date

        _start_date_time = _start_date

        if _start_time is not None:
            _start_date_time = datetime.datetime.combine(
                _start_date_time, _start_time
            )

        if _timezone is not None:
            _start_date_time = _timezone.localize(_start_date_time)

        _start_date_time = _start_date_time.astimezone(pytz.UTC)
        return _start_date_time

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
            _eta = self._combine_date_time(**schedule)
            if current_datetime <= _eta:
                eta = _eta
                break  # exit
        return eta

    @timezone_required
    @validate_cron
    def get_next_eta_cron(self, timezone=None, _format=None, from_date=None, start_date=None, start_time=None, end_date=None, end_time=None, cron=None):
        """
        Which returns the next eta based on cron config
        """
        eta = None
        start_date_time = self._combine_date_time(
            timezone=timezone, start_date=start_date, start_time=start_time, _format=_format)
        end_date_time = self._combine_date_time(
            timezone=timezone, start_date=end_date, start_time=end_time, _format=_format)
        from_date_time = self._combine_date_time(
            timezone=timezone, start_date=from_date, _format=_format)
        current_date_time = datetime.datetime.now().astimezone(pytz.UTC)
        # from_date -> start_date (precedence)
        if from_date_time is None:
            if start_date_time is None:
                from_date_time = current_date_time
            else:
                from_date_time = start_date_time
        if from_date_time < current_date_time:
            from_date_time = current_date_time
        cronifier = croniter(cron, from_date_time)
        eta = cronifier.get_next(datetime.datetime).astimezone(pytz.UTC)
        if eta is not None and end_date_time is not None:
            if eta > end_date_time:
                return None  # exit
        return eta  # exit

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
        schedule_type = schedule_data.get('schedule_type', None)
        timezone = schedule_data.get('timezone', None)
        _format = schedule_data.get('_format', '%m/%d/%Y')

        if timezone is not None:
            if timezone not in TIMEZONES:
                raise ValueError("Invalid timezone {}".format(timezone))

        eta = None

        if schedule_type.lower() == 'date_specific':
            schedules = schedule_data.get('schedules', [])
            eta = self.get_next_eta_date_specific(
                timezone=timezone, _format=_format, schedules=schedules)

        start_date = schedule_data.get('start_date', None)
        start_time = schedule_data.get('start_time', None)
        end_date = schedule_data.get('end_date', None)
        end_time = schedule_data.get('end_time', None)

        if schedule_type.lower() == 'cron':
            cron = schedule_data.get('cron', None)
            if not isinstance(cron, str):
                raise TypeError("invalid cron")
            if not croniter.is_valid(cron):
                raise ValueError("Invalid cron specified {}".format(cron))
            eta = self.get_next_eta_cron(
                timezone=timezone, _format=_format, start_date=start_date, start_time=start_time, end_date=end_date, end_time=end_time, cron=cron)

        if schedule_type.lower() == 'recurring':
            recurring = schedule_data.get('recurring', {})
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
