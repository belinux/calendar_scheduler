# calendar_scheduler
Google like calendar scheduler. Finding eta with given parameters is bit of challenging. I found a pattern here and made it as a package.

# Installation

    pip3 install calendar_scheduler

# Usage
There are two types of schedulers that you can use of.<br>
### 1. Date Specific<br >
You can specify single or multiple dates to get the eta referenced to current datetime


    from scheduler import Scheduler

    scheduler = Scheduler()

    payload = {
        "schedule_type": "date_specific",
        "timezone": "<valid_timezone>",
        "schedules": [
            {
                "start_date": "<mm/dd/yyyy>*",
                "start_time": "<HH:MM AM/PM>*"
            }
        ]
    }
    eta = scheduler.get_next_eta(schedule_data=payload)
    # This will return UTC converted eta (datetime obj)
    # You can specify multiple dates

### 2. Recurring using `cron`
You can specify a cron to get next eta with respect to current datetime and also you can specify the base datetime.

    from scheduler import Scheduler

    scheduler = Scheduler()

    payload = {
        "schedule_type": "cron",
        "timezone": "<valid_timezone>",
        "start_date": "<mm/dd/yyyy>*",
        "start_time": "<HH:MM AM/PM>*",
        "end_date": "[<mm/dd/yyyy>]",
        "end_time": "[<HH:MM AM/PM>]",
        "cron": "<cron expression>"
    }
    eta = scheduler.get_next_eta(schedule_data=payload)
    # This will return UTC converted eta (datetime obj)

# Unit Tests

    python3 -m unittest discover scheduler.tests

# Coverage

    coverage run -m unittest discover scheduler.tests
    coverage report

# Cron generator
https://www.npmjs.com/package/cron-builder

# Thanks
https://github.com/kiorky/croniter

# How to contribute

- Submit a PR, welcome for feedback

Note: I've been using package from so long and it is in production. 

# Author
Partha saradhi Konda <parthasaradhi1992@gmail.com>