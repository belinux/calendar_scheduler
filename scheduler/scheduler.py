"""
Scheduler
"""

__author__ = "Partha Saradhi Konda<parthasaradhi1992@gmail.com>"
__version__ = 0.1

import datetime
import pytz
from enums import TIMEZONES


class Scheduler(object):

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
        if timezone not in TIMEZONES:
            raise ValueError(
                "Invalid timezone provided {}".format(timezone))  # exit
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

    def get_next_eta_cron(self, timezone=None, _format=None, start_date=None, start_time=None, end_date=None, end_time=None, cron=None):
        """
        Which returns the next eta based on cron config
        """
        pass

    def get_next_eta_recurring(self, timezone=None, _format=None, start_date=None, start_time=None, end_date=None, end_time=None, recurring={}):
        """
        Which return the next eta based on the recurring info
        """
        pass

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

        if schedule_type is None or schedule_type.lower() not in ['date_specific', 'cron', 'recurring']:
            raise ValueError("Invalid Scheduling Type")
        if timezone is None:
            raise ValueError("timezone is required")
        if timezone not in TIMEZONES:
            raise ValueError("Invalid timezone {}".format(timezone))

        if schedule_type.lower() == 'date_specific':
            eta = self.get_next_eta_date_specific(
                timezone=timezone, _format=_format, schedules=schedules)

        if schedule_type.lower() in ['cron', 'recurring']:
            if not schedules:
                raise ValueError("Invalid schedules")
            if len(schedules) > 1:
                raise ValueError("schedules must be length 1")
            if not isinstance(schedules[0], dict):
                raise ValueError("Invalid scheduling info")
            start_date = schedules[0].get('start_date', None)
            start_time = schedules[0].get('start_time', None)

        if schedule_type.lower() == 'cron':
            eta = self.get_next_eta_cron(timezone=timezone, start_date=start_date,
                                         start_time=start_time, end_date=end_date, end_time=end_time, cron=cron)
        if schedule_type.lower() == 'recurring':
            if not recurring:
                raise ValueError("recurring is required")
            if not isinstance(recurring, dict):
                raise TypeError("Invalid recurring")
            eta = self.get_next_eta_recurring(
                timezone=timezone, start_date=start_date,
                start_time=start_time, end_date=end_date, end_time=end_time,
                recurring=recurring)
        return eta
