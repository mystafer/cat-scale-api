from datetime import datetime, timedelta

from query_ddb import query_events
from utils import parse_local_date_key, date_to_query_key

MAX_DURATION = timedelta(days=31) 

def get_cat_events(cats, start_date_s, end_date_s, dynamodb):
    start_date = parse_local_date_key(start_date_s, True)
    end_date = parse_local_date_key(end_date_s, False)

    if (end_date < start_date):
        raise ValueError("End Date should be after Start Date")
    if (end_date - start_date > MAX_DURATION):
        raise ValueError("Date Range should be less than one month")

    # get all events in range, include one day prior to ensure all events queried
    events = []
    date = start_date
    last_date = end_date + timedelta(seconds=1)
    while date <= last_date:
        query_key = date_to_query_key(date)
        events += query_events(query_key, dynamodb)
        date += timedelta(days=1)

    # loop over all cats and split events
    for cat in cats:

        # locate events for cat sorted in revers chron order
        start_ts = int(start_date.timestamp() * 1000)
        end_ts = int(end_date.timestamp() * 1000)
        cat_events = sorted(
            [e for e in events if e['cat'] == cat['name'] and e['event_data']['timestamp'] >= start_ts and e['event_data']['timestamp'] < end_ts ],
            key=lambda x: x['event_data']['timestamp'])

        cat['events'] = cat_events
