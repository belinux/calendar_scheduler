"""
Scheduler
"""

__author__ = "Partha Saradhi Konda<parthasaradhi1992@gmail.com>"
__version__ = 0.1

import datetime
import pytz
from .enums import TIMEZONES


class Scheduler(object):

    def get_schedule_eta(self, schedule_timezone=None, schedule_date=None, schedule_time=None, _format=None):
        """
        Will convert the given datetime into UTC
        If timezone is also provided, then it will localize and convert it into the UTC
        @schedule_timezone: timezone from given list
        @schedule_date: string or dateinstance (dd/mm/yyyy or mm/dd/yyy or mm-dd-yyy or dd-mm-yyyy)
        @schedule_time: string (HH:MM AM/PM/am/pm)
        @_format: dd/mm/yyyy or mm/dd/yyy or mm-dd-yyy or dd-mm-yyyy or parsable format
        """
        _schedule_date_time = None
        _schedule_time = None
        _schedule_timezone = None

        if schedule_timezone is not None:
            if schedule_timezone not in TIMEZONES:
                raise ValueError("Invalid Timezone")  # exit
        if schedule_date is None:
            raise ValueError("schedule_date should not be null")  # exit
        if schedule_time is None:
            raise ValueError("schedule_time should not be null")  # exit

        # Parse time into timeobj
        try:
            _schedule_time = datetime.datetime.strptime(
                schedule_time, "%H:%M %p")
        except ValueError:
            raise ValueError(
                "Unable to parse the time {}".format(schedule_time))

        # Convert given format of date to date instance
        if not isinstance(schedule_date, datetime.date):
            if not isinstance(schedule_date, str):
                raise TypeError("Invalid type of date")  # exit
            if _format is None:
                raise ValueError("_format is not provided")  # exit
            try:
                schedule_date = datetime.datetime.strptime(
                    schedule_date, _format)
            except ValueError:
                raise ValueError("Invalid format {}".format(_format))
        _schedule_date_time = datetime.datetime.combine(
            schedule_date, _schedule_time
        )

        _schedule_timezone = pytz.timezone(schedule_timezone)

        # Localize and convert to UTC
        if _schedule_timezone is not None:
            _schedule_date_time = _schedule_timezone.localize(
                _schedule_date_time)

        _schedule_date_time = _schedule_date_time.astimezone(pytz.UTC)
        return _schedule_date_time  # exit

    def get_schedule_eta_multiple(self, schedule_timezone=None, schedule_data=[]):
        """
        For multiple date & times
        return: eta/None, message/None
        """
        eta = None
        if not schedule_data:
            raise ValueError("schedule info not provided")  # exit
        if schedule_timezone not in TIMEZONES:
            raise ValueError(
                "Invalid timezone provided {}".format(schedule_timezone))  # exit

        current_datetime = datetime.datetime.now().astimezone(pytz.UTC)

        for schedule in schedule_data:
            if 'schedule_date' not in schedule and 'schedule_time' not in schedule:
                raise ValueError(
                    "schedule_date & schedule_time are required in schedule_date")  # exit
            if schedule_timezone is not None:
                schedule.update({
                    'schedule_timezone': schedule_timezone})
            # unpack won't cause arg error
            try:
                _eta = self.get_schedule_eta(**schedule)
                if current_datetime <= _eta:
                    eta = _eta
                    break
            except ValueError as v:
                continue
        return eta

    def get_recurring_schedule_eta(self, schedule_data={}):
        """
        """
        pass

    def get_recurring_schedules_eta(self, schedule_data={}, of_next='w'):
        """
        """
        pass
