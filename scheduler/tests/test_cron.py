import datetime
import pytz
import unittest
from ..scheduler import Scheduler


class TestDateSpecific(unittest.TestCase):

    def test_scheduler_empty_cron(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            eta = scheduler.get_next_eta(schedule_data={
                'schedule_type': 'cron',
                'timezone': 'Asia/Calcutta',
                'cron': ''
            })

    def test_scheduler_invalid_cron(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            eta = scheduler.get_next_eta(schedule_data={
                'schedule_type': 'cron',
                'timezone': 'Asia/Calcutta',
                'cron': '* x u s'
            })

    def test_scheduler_valid_cron(self):
        scheduler = Scheduler()
        current_datetime = datetime.datetime.now().astimezone(pytz.UTC)
        ground_offset = current_datetime.minute % 5
        delta = datetime.timedelta(minutes=(5 - ground_offset))
        next_5_minutes = current_datetime + delta
        next_5_minutes = next_5_minutes.replace(second=0, microsecond=0)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'cron',
            'timezone': 'Asia/Calcutta',
            'cron': '*/5 * * * *'
        })
        self.assertEqual(eta, next_5_minutes)

    def test_scheduler_valid_cron_with_end_date(self):
        scheduler = Scheduler()
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'cron',
            'timezone': 'Asia/Calcutta',
            'cron': '*/5 * * * *',
            'end_date': '2/02/2018',
            'end_time': '12:23 PM'
        })
        self.assertIsNone(eta)

    def test_scheduler_valid_cron_with_prev_start_date(self):
        scheduler = Scheduler()
        current_datetime = datetime.datetime.now().astimezone(pytz.UTC)
        ground_offset = current_datetime.minute % 5
        delta = datetime.timedelta(minutes=(5 - ground_offset))
        next_5_minutes = current_datetime + delta
        next_5_minutes = next_5_minutes.replace(second=0, microsecond=0)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'cron',
            'timezone': 'Asia/Calcutta',
            'cron': '*/5 * * * *',
            'start_date': '1/20/2019',
            'start_time': '12:23 PM'
        })
        self.assertEqual(eta, next_5_minutes)

    def test_scheduler_valid_cron_with_next_start_date(self):
        scheduler = Scheduler()
        expected_datetime = datetime.datetime.strptime(
            '03/20/2099 12:25 PM', '%m/%d/%Y %I:%M %p').astimezone(pytz.UTC)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'cron',
            'timezone': 'Asia/Calcutta',
            'cron': '*/5 * * * *',
            'start_date': '3/20/2099',
            'start_time': '12:23 PM'
        })
        self.assertEqual(eta, expected_datetime)
