from datetime import datetime, timedelta, timezone

DEFAULT_VISIT_COLLAPSE_MS = 15 * 60 * 1000 # 15 min
EVENT_VISIT_COLLAPSE_MS = 2 * 60 * 1000 # 2 min

def collapse_events_to_visits(events, visit_collapse_ms=DEFAULT_VISIT_COLLAPSE_MS):
    """ events should be ordered already chronologically, this function will consolidate nearby events into visits """

    # initialize visits list as empty and last timestamp to zero to force first event to create a visit
    visits = []
    visit = { 'end_timestamp': 0 }

    # loop over events and look for visits to collapse
    for event in events:

        event_ts = event['event_data']['timestamp']
        distance_ms = event_ts - visit['end_timestamp']  # collapse events together from end of interval

        # event is close, collapse it to current visit
        if distance_ms <= EVENT_VISIT_COLLAPSE_MS:
            visit['events'].append(event)
            visit['end_timestamp'] = event_ts

        # event is far, start a new visit
        else:
            visit = {
                'tick': event_ts,
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
        visit['events'] = [ e['event_data']['timestamp'] for e in visit['events'] ]

    # collapse again if necessary
    if visit_collapse_ms > 0 and len(visits) > 0:
        
        # determine the starting and ending tick markers
        first_visit = visits[0]
        last_visit = visits[-1]

        dt = datetime.utcfromtimestamp(int(first_visit['start_timestamp']/1000))
        start_ts = int(dt.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc).timestamp() * 1000)

        dt = datetime.utcfromtimestamp(int(last_visit['end_timestamp']/1000))
        end_ts = int(dt.replace(hour=23, minute=59, second=59, microsecond=999, tzinfo=timezone.utc).timestamp() * 1000) + visit_collapse_ms

        # loop over tick markers and collapse visits to nearest ticks
        remaining_visits = visits.copy()
        next_visit = remaining_visits.pop(0)
        collapsed_visits = []
        half_tick_ts = int(max(visit_collapse_ms / 2, 1))
        for tick_ts in range(start_ts, end_ts, visit_collapse_ms):

            # calculate range of event timestamps to include in tick
            tick_start_ts = tick_ts - half_tick_ts
            tick_end_ts = tick_ts + half_tick_ts

            # loop over visits neer tick and collapse them
            collapsed_visit = None
            while next_visit:

                # skip to next tick if visit is not part of it
                if tick_start_ts > next_visit['start_timestamp'] or next_visit['start_timestamp'] >= tick_end_ts:
                    break

                visit_ts = next_visit['start_timestamp']

                # collapse it to current visit
                if collapsed_visit != None:
                    collapsed_visit['collapsed_visits'].append(event)
                    collapsed_visit['end_timestamp'] = visit_ts
                    collapsed_visit['elapsed_sec'] += next_visit['elapsed_sec']
                    collapsed_visit['total_collapsed'] += 1

                # start a new visit
                else:
                    collapsed_visit = {
                        'tick': tick_ts,
                        'start_timestamp': visit_ts,
                        'end_timestamp': visit_ts,
                        'collapsed_visits': [ next_visit ],
                        'elapsed_sec': next_visit['elapsed_sec'],
                        'total_collapsed': 1
                    }
                    collapsed_visits.append(collapsed_visit)

                next_visit = remaining_visits.pop(0) if len(remaining_visits) > 0 else None

            if not next_visit:
                break

        return collapsed_visits

    return visits
