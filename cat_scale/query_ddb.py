import boto3
from boto3.dynamodb.conditions import Key

def query_events(keys, dynamodb):
    """ query DynamoDB log table for entries matching keys """

    table = dynamodb.Table('cat_scale_event')
    if 'sk' in keys:
        response = table.query(
            KeyConditionExpression=Key('sample_date').eq(keys['pk'])
                & Key('sample_time').between(*keys['sk'])
        )
    else:
        response = table.query(
            KeyConditionExpression=Key('sample_date').eq(keys['pk'])
        )

    # hack to ensure date_est renamed to date_local
    events = response['Items']
    old_events = [e for e in events if 'date_local' not in e['event_data']]
    for event in old_events:
        event['event_data']['date_local'] = event['event_data'].pop('date_est')

    return events

def query_for_cats(dynamodb):
    """ query DynamoDB log table for cats """

    table = dynamodb.Table('cat_scale_settings')
    response = table.query(
        KeyConditionExpression=Key('setting_type').eq('cat-definition')
    )
    
    cats = response['Items']

    # return in a consistent order
    cats.sort(key=lambda x: x['name'])

    return cats
