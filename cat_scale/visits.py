
DEFAULT_EVENT_VISIT_COLLAPSE_MS = 2 * 60 * 1000 # 2 min

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
