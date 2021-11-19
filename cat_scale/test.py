import boto3
from query_ddb import query_for_cats
from events import get_cat_events
from visits import collapse_events_to_visits


if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb')

    try:
        cats = query_for_cats(dynamodb)
        get_cat_events(cats, '2021.11.18', '2021.11.18', dynamodb)
        
        for cat in cats:
            print(f"{cat['name']} -> {len(cat['events'])}")

            visits = collapse_events_to_visits(cat['events'])
            print(f"{len(visits)} -> {sum([v['total_collapsed'] for v in visits])}")

    except ValueError as err:
        print(err)