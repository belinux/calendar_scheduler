import datetime
import pytz
import unittest
from scheduler import Scheduler


class TestDateSpecific(unittest.TestCase):
    def test_scheduler_no_data(self):
        scheduler = Scheduler()
        with self.assertRaises(ValueError):
            scheduler.get_next_eta()
