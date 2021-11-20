from datetime import datetime, timedelta, timezone

from query_ddb import query_events
from utils import parse_local_date_key, date_to_query_key, get_datetime_for_event_ts, get_local_ts_for_event_ts, TZ_LOCAL

MAX_DURATION = timedelta(days=31) 

def get_cat_events(cats, start_date_s, end_date_s, dynamodb):
    start_date_local = parse_local_date_key(start_date_s, True)
    end_date_local = parse_local_date_key(end_date_s, False)

    if (end_date_local < start_date_local):
        raise ValueError("End Date should be after Start Date")
    if (end_date_local - start_date_local > MAX_DURATION):
        raise ValueError("Date Range should be less than one month")

    start_date = start_date_local.replace(tzinfo=None)
    start_date -= TZ_LOCAL.utcoffset(start_date)
    end_date = end_date_local.replace(tzinfo=None)
    end_date -= TZ_LOCAL.utcoffset(end_date)

    # get all events in range, include one day prior and after to ensure all events queried
    events = []
    date = start_date - timedelta(days=1)
    last_date = end_date + timedelta(days=1, seconds=1)
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
            [e for e in events if e['cat'] == cat['name'] and (start_ts <= get_local_ts_for_event_ts(e) < end_ts) ],
            key=lambda x: x['event_data']['timestamp'])

        cat['events'] = cat_events
