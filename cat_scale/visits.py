
# TZ_LOCAL = tz.timezone('America/New_York')
# DEFAULT_VISIT_COLLAPSE_MS = 15 * 60 * 1000 # 15 min
DEFAULT_EVENT_VISIT_COLLAPSE_MS = 2 * 60 * 1000 # 2 min
# TWENTY_FOUR_HOURS = 24 * 60 * 60 * 1000 # 24 hours
# MAX_DURATION = timedelta(days=31) 

def collapse_events_to_visits(events, visit_collapse_ms=DEFAULT_EVENT_VISIT_COLLAPSE_MS, keep_events=False):
    """ events should be ordered already chronologically, this function will consolidate nearby events into visits """

    # initialize visits list as empty and last timestamp to zero to force first event to create a visit
    visits = []
    visit = { 'end_timestamp': 0 }

    # loop over events and look for visits to collapse
    for event in events:

        event_ts = event['event_data']['timestamp']
        distance_ms = event_ts - visit['end_timestamp']  # collapse events together from end of interval

        # event is close, collapse it to current visit
        if distance_ms <= visit_collapse_ms:
            visit['events'].append(event)
            visit['end_timestamp'] = event_ts
            visit['total_collapsed'] += 1

        # event is far, start a new visit
        else:
            visit = {
                'start_timestamp': event_ts,
                'end_timestamp': event_ts,
                'events': [ event ],
                'total_collapsed': 1
            }
            visits.append(visit)

    # loop over visits and reduce size
    for visit in visits:
        last_event = visit['events'][-1]
        visit['elapsed_sec'] = (visit['end_timestamp'] - visit['start_timestamp']) / 1000 + last_event['event_data']['elapsed_sec']

        if keep_events == False:
            visit['events'] = [ e['event_data']['timestamp'] for e in visit['events'] ]

    return visits

    # # collapse again if necessary
    # if visit_collapse_ms > 0 and len(visits) > 0:
        
    #     # determine the starting and ending tick markers
    #     first_visit = visits[0]
    #     last_visit = visits[-1]

    #     dt = datetime.utcfromtimestamp(int(first_visit['start_timestamp']/1000))
    #     start_ts = int(dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp() * 1000)

    #     dt = datetime.utcfromtimestamp(int(last_visit['end_timestamp']/1000))
    #     end_ts = int(dt.replace(hour=23, minute=59, second=59, microsecond=999, tzinfo=timezone.utc).timestamp() * 1000) + visit_collapse_ms

    #     # loop over tick markers and collapse visits to nearest ticks
    #     remaining_visits = visits.copy()
    #     next_visit = remaining_visits.pop(0)
    #     collapsed_visits = []
    #     for tick_ts in range(start_ts, end_ts, visit_collapse_ms):

    #         # calculate range of event timestamps to include in tick
    #         tick_start_ts = tick_ts

    #         # if collapsing by days, move start back to nearest day
    #         if (visit_collapse_ms >= TWENTY_FOUR_HOURS):
    #             dt = datetime.utcfromtimestamp(int(tick_start_ts/1000))
    #             offset = TZ_LOCAL.utcoffset(dt)
    #             dt += offset
    #             dt.replace(hour=0, minute=0, second=0, microsecond=0)
    #             tick_start_ts = int(dt.astimezone(timezone.utc).timestamp() * 1000)

    #         # calculate end range of tick
    #         tick_end_ts = tick_ts + visit_collapse_ms

    #         # loop over visits neer tick and collapse them
    #         collapsed_visit = None
    #         while next_visit:

    #             # skip to next tick if visit is not part of it
    #             if tick_start_ts > next_visit['start_timestamp'] or next_visit['start_timestamp'] >= tick_end_ts:
    #                 break

    #             visit_ts = next_visit['start_timestamp']

    #             # collapse it to current visit
    #             if collapsed_visit != None:
    #                 collapsed_visit['collapsed_visits'].append(event)
    #                 collapsed_visit['end_timestamp'] = visit_ts
    #                 collapsed_visit['elapsed_sec'] += next_visit['elapsed_sec']
    #                 collapsed_visit['total_collapsed'] += 1

    #             # start a new visit
    #             else:
    #                 collapsed_visit = {
    #                     'tick': tick_start_ts,
    #                     'start_timestamp': visit_ts,
    #                     'end_timestamp': visit_ts,
    #                     'collapsed_visits': [ next_visit ],
    #                     'elapsed_sec': next_visit['elapsed_sec'],
    #                     'total_collapsed': 1
    #                 }
    #                 collapsed_visits.append(collapsed_visit)

    #             next_visit = remaining_visits.pop(0) if len(remaining_visits) > 0 else None

    #         if not next_visit:
    #             break

    #     return collapsed_visits

    # return visits











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
