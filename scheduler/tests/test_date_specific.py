import datetime
import pytz
import unittest
from scheduler import Scheduler


class TestDateSpecific(unittest.TestCase):
    def test_scheduler_no_data(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.get_next_eta()

    def test_scheduler_no_schedule_type(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.get_next_eta(schedule_data={
                'schedule_type': ''
            })

    def test_scheduler_invalid_schedule_type(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.get_next_eta(schedule_data={
                'schedule_type': 'asdf'
            })

    def test_scheduler_valid_schedule_type(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.get_next_eta(schedule_data={
                'schedule_type': 'date_specific'
            })

    def test_scheduler_valid_timezone(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.get_next_eta(schedule_data={
                'schedule_type': 'date_specific',
                'timezone': 'Asia/Calcutta'
            })

    def test_scheduler_previous_date_single(self):
        scheduler = Scheduler()
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'date_specific',
            'timezone': 'Asia/Calcutta',
            'schedules': [
                {
                    'start_date': '02/20/2019',
                    'start_time': "12:24 PM"
                }
            ]
        })
        self.assertIsNone(eta)

    def test_scheduler_forward_date_single(self):
        scheduler = Scheduler()
        expected_datetime = datetime.datetime.strptime(
            '02/20/2099 12:24 PM', '%m/%d/%Y %H:%M %p').astimezone(pytz.UTC)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'date_specific',
            'timezone': 'Asia/Calcutta',
            'schedules': [
                {
                    'start_date': '02/20/2099',
                    'start_time': "12:24 PM"
                }
            ]
        })
        self.assertEqual(eta, expected_datetime)

    def test_scheduler_previous_date_multiple(self):
        scheduler = Scheduler()
        expected_datetime = datetime.datetime.strptime(
            '02/20/2012 12:24 PM', '%m/%d/%Y %H:%M %p').astimezone(pytz.UTC)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'date_specific',
            'timezone': 'Asia/Calcutta',
            'schedules': [
                {
                    'start_date': '01/20/2019',
                    'start_time': "12:24 PM"
                },
                {
                    'start_date': '02/20/2019',
                    'start_time': "12:24 PM"
                }
            ]
        })
        self.assertIsNone(eta)

    def test_scheduler_forward_date_multiple(self):
        scheduler = Scheduler()
        expected_datetime = datetime.datetime.strptime(
            '01/20/2099 12:24 PM', '%m/%d/%Y %H:%M %p').astimezone(pytz.UTC)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'date_specific',
            'timezone': 'Asia/Calcutta',
            'schedules': [
                {
                    'start_date': '01/20/2099',
                    'start_time': "12:24 PM"
                },
                {
                    'start_date': '02/20/2099',
                    'start_time': "12:24 PM"
                }
            ]
        })
        self.assertEqual(eta, expected_datetime)

    def test_scheduler_previous_forward_date_multiple(self):
        scheduler = Scheduler()
        expected_datetime = datetime.datetime.strptime(
            '02/20/2099 12:24 PM', '%m/%d/%Y %H:%M %p').astimezone(pytz.UTC)
        eta = scheduler.get_next_eta(schedule_data={
            'schedule_type': 'date_specific',
            'timezone': 'Asia/Calcutta',
            'schedules': [
                {
                    'start_date': '01/20/2019',
                    'start_time': "12:24 PM"
                },
                {
                    'start_date': '02/20/2099',
                    'start_time': "12:24 PM"
                }
            ]
        })
        self.assertEqual(eta, expected_datetime)

    def test_date_specific(self):
        scheduler = Scheduler()
        payload = {
            "schedule_type": "<date_specific|recurring|cron>*",
            "timezone": "<valid_timezone>*",
            "schedules": [
                {
                    "start_date": "<mm/dd/yyyy>*",
                    "start_time": "<HH:MM AM/PM>*"
                }
            ],
            "end_date": "[<mm/dd/yyyy>]",
            "end_time": "[<HH:MM AM/PM>]",
            "recurring": {
                "run_on_monday": "<true|false>",
                "run_on_tuesday": "<true|false>",
                "run_on_wednesday": "<true|false>",
                "run_on_thursday": "<true|false>",
                "run_on_friday": "<true|false>",
                "repeat_every": "<any number>",
                "repeat_every_unit": "<m|h|d|w|M|y>",
                "repeat_for": "<any number>",
                "repeat_for_unit": "<m|h|d|w|M|y>"
            },
            "cron": "<cron expression> : required if schedule_type is cron"
        }
