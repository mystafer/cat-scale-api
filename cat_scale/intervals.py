from datetime import datetime, timedelta, timezone

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
    dt = dt.astimezone(TZ_LOCAL)
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # dt += dt.tzinfo.utcoffset(dt)
    dt += timedelta(days=-1)
    print(f"collapse_visits_to_intervals start {dt}")
    start_ts = int(dt.timestamp() * 1000)

    dt = datetime.utcfromtimestamp(int(last_visit['end_timestamp']/1000))
    dt = dt.astimezone(TZ_LOCAL)
    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    # dt += dt.tzinfo.utcoffset(dt)
    dt += timedelta(days=2)
    print(f"collapse_visits_to_intervals end {dt}")
    end_ts = int(dt.timestamp() * 1000)

    # skip visits before starting tick
    remaining_visits = visits.copy()
    next_visit = remaining_visits.pop(0)
    while next_visit:
        if start_ts <= next_visit['start_timestamp']:
            break

        print(f"skip {start_ts}")
        next_visit = remaining_visits.pop(0)

    # loop over tick markers and collapse visits to nearest ticks
    intervals = []
    if next_visit:
        for tick_ts in range(start_ts, end_ts, interval_collapse_ms):

            # calculate range of event timestamps to include in tick
            tick_start_ts = tick_ts

            # calculate end range of tick
            tick_end_ts = tick_ts + interval_collapse_ms

            # loop over visits neer tick and collapse them
            interval = None
            while next_visit:

                if next_visit['start_timestamp'] >= tick_end_ts:
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

                    # # correct tick tz offset for display
                    # dt = datetime.utcfromtimestamp(int(tick_start_ts/1000))
                    # dt += TZ_LOCAL.utcoffset(dt)
                    # tick = int(dt.timestamp() * 1000)
                    tick = tick_start_ts

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
