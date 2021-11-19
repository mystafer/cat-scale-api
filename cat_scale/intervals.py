from datetime import datetime, timezone

from utils import TZ_LOCAL

DEFAULT_VISIT_INTERVAL_COLLAPSE_MS = 15 * 60 * 1000 # 15 min
TWENTY_FOUR_HOURS = 24 * 60 * 60 * 1000 # 24 hours

def collapse_visits_to_intervals(visits, interval_collapse_ms=DEFAULT_VISIT_INTERVAL_COLLAPSE_MS):
    """ visits should be ordered already chronologically, this function will consolidate nearby visits into intervals """

    if (len(visits) == 0):
        return []

    # determine the starting and ending tick markers
    first_visit = visits[0]
    last_visit = visits[-1]

    dt = datetime.utcfromtimestamp(int(first_visit['start_timestamp']/1000))
    start_ts = int(dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp() * 1000)

    dt = datetime.utcfromtimestamp(int(last_visit['end_timestamp']/1000))
    end_ts = int(dt.replace(hour=23, minute=59, second=59, microsecond=99999, tzinfo=timezone.utc).timestamp() * 1000) + DEFAULT_VISIT_INTERVAL_COLLAPSE_MS

    # loop over tick markers and collapse visits to nearest ticks
    remaining_visits = visits.copy()
    next_visit = remaining_visits.pop(0)
    intervals = []
    for tick_ts in range(start_ts, end_ts, interval_collapse_ms):

        # calculate range of event timestamps to include in tick
        tick_start_ts = tick_ts

        # if collapsing by days, move start back to nearest day
        if (interval_collapse_ms >= TWENTY_FOUR_HOURS):
            dt = datetime.utcfromtimestamp(int(tick_start_ts/1000))
            offset = TZ_LOCAL.utcoffset(dt)
            dt += offset
            dt.replace(hour=0, minute=0, second=0, microsecond=0)
            tick_start_ts = int(dt.astimezone(timezone.utc).timestamp() * 1000)

        # calculate end range of tick
        tick_end_ts = tick_ts + interval_collapse_ms

        # loop over visits neer tick and collapse them
        interval = None
        while next_visit:

            # skip to next tick if visit is not part of it
            if tick_start_ts > next_visit['start_timestamp'] or next_visit['start_timestamp'] >= tick_end_ts:
                break

            visit_ts = next_visit['start_timestamp']

            # collapse it to current interval
            if interval != None:
                interval['visits'].append(next_visit)
                interval['end_timestamp'] = visit_ts
                interval['elapsed_sec'] += next_visit['elapsed_sec']
                interval['total_collapsed'] += 1

            # start a new interval
            else:

                # correct tick tz offset for display
                dt = datetime.utcfromtimestamp(int(tick_start_ts/1000))
                offset = TZ_LOCAL.utcoffset(dt)
                if (interval_collapse_ms >= TWENTY_FOUR_HOURS):
                    dt -= 2 * offset
                else:
                    dt -= offset
                tick = int(dt.astimezone(timezone.utc).timestamp() * 1000)

                interval = {
                    'tick': tick,
                    'start_timestamp': visit_ts,
                    'end_timestamp': visit_ts,
                    'visits': [ next_visit ],
                    'elapsed_sec': next_visit['elapsed_sec'],
                    'total_collapsed': 1
                }
                intervals.append(interval)

            next_visit = remaining_visits.pop(0) if len(remaining_visits) > 0 else None

        if not next_visit:
            break

    return intervals











# def get_cat_visits(start_date_s, end_date_s, collapse_ms=DEFAULT_VISIT_COLLAPSE_MS, dynamodb=None):
#     """ fetch visits from DynamoDB for a date range """
#     if not dynamodb:
#         dynamodb = boto3.resource('dynamodb')

#     start_date = datetime.strptime(start_date_s, "%Y.%m.%d")
#     end_date = datetime.strptime(end_date_s, "%Y.%m.%d")

#     if (end_date < start_date):
#         raise ValueError("End Date should be after Start Date")
#     if (end_date - start_date > MAX_DURATION):
#         raise ValueError("Date Range should be less than one month")

#     # get all events in range, include one day prior to ensure all events queried
#     events = []
#     date = start_date - timedelta(days=1)
#     last_date = end_date + timedelta(days=1)
#     while date <= last_date:
#         events += query_events({ 'pk': datetime.strftime(date, "%Y.%m.%d") }, dynamodb)
#         date += timedelta(days=1)

#     # loop over all cats and split events
#     cats = []
#     for c in query_for_cats(dynamodb):

#         # initialize cat object
#         cat = {
#             'id': c['setting_id'],
#             'name': c['name']
#         }

#         # locate events for cat sorted in revers chron order
#         cat_events = sorted(
#             [e for e in events if e['cat'] == cat['name']],
#             key=lambda x: x['event_data']['timestamp'])

#         # visits = collapse_events_to_visits(cat_events, collapse_ms)
#         visits = collapse_events_to_visits(cat_events)

#         # remove initial event if before start date
#         if len(visits) > 0 and visits[0]['tick'] < start_date.timestamp() * 1000:
#             visits = visits[1:]

#         # remove last event if after end date
#         if len(visits) > 0 and visits[-1]['tick'] > end_date.timestamp() * 1000:
#             visits = visits[:-1]

#         cat['visits'] = visits

#         cats.append(cat)

#     return cats
