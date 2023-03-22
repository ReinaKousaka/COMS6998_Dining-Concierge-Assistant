''' Documentation:
SQS: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
SMS: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html
'''
import json
import logging
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
from botocore.vendored import requests
from botocore.exceptions import ClientError

# AWS related consts
REGION = 'us-east-1'
HOST = 'PRIVATE-HOST-URL'
INDEX = 'restaurants'
QUEUE_URL = 'PRIVATE-QUEUE-URL'
SLOTS = ['Cuisine', 'NumPeople', 'Location', 'Date', 'Time', 'PhoneNum']

# App realted consts
NUM_RECOMMENDATIONS = 3


# Poll message from SQS
def poll_SQS():
    logging.error("Getting messages from Queue")
    client = boto3.client('sqs')
    response = client.receive_message(
        QueueUrl=QUEUE_URL,
        AttributeNames=['SentTimestamp'],
        MessageAttributeNames=['ALL'],
        MaxNumberOfMessages=1,
        VisibilityTimeout=0,
        WaitTimeSeconds=0
    )
    return response
    # if response:
    #     preferences = dict.fromkeys(SLOTS)
    #     for slotName in SLOTS:
    #         preferences[slotName] = response['Messages'][0]["MessageAttributes"][slotName]["StringValue"]
    #     return preferences, response['Messages'][0]['ReceiptHandle']


# OpenSearch query the cuisine, use DB info to ensure location requirements are satisfied
def get_restaurant_recommendation(cuisine, location):
    def get_awsauth(region, service):
        cred = boto3.Session().get_credentials()
        return AWS4Auth(
            cred.access_key,
            cred.secret_key,
            region,
            service,
            session_token=cred.token
        )

    logging.error(f'OpenSearch Querying {cuisine}')
    SEARCH_RANGE = 100
    client = OpenSearch(
        hosts=[{
            'host': HOST,
            'port': 443
        }],
        http_auth=get_awsauth(REGION, 'es'),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    
    response = client.search(
        index=INDEX,
        body={
            'size': SEARCH_RANGE,
            'query': {'multi_match': {'query': cuisine}}
        }
    )

    res = []
    for i in range(SEARCH_RANGE):
        if len(res) >= NUM_RECOMMENDATIONS: break
        Business_ID = response['hits']['hits'][i]['_source']['restaurants']
        restaurant_info = fetch_restaurant(Business_ID)
        if restaurant_info['city_region'].lower() == location.lower():
            res.append(restaurant_info)
    return res

# get restaurant info from DB
def fetch_restaurant(Business_ID):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('yelp-restaurants')

    response_from_DB = table.query(
        KeyConditionExpression=Key('Business_ID').eq(Business_ID)
    )
    res = {
        'name': response_from_DB['Items'][0]['Name'],
        'address': response_from_DB['Items'][0]['Address'],
        'city_region': response_from_DB['Items'][0]['City_Region']
    }
    return res


def send_SMS(phone_num, message):
    logging.error(f'start sending SMS')
    client = boto3.client('sns')
    # phone number should be in E.164 format.
    response = client.publish(
        PhoneNumber=f'+1{phone_num}',
        Message=message
    )


def send_SES(email_address, message):
    EMAIL_SENDER = 'sy3079@columbia.edu'
    EMAIL_RECIPIENT = email_address
    CHARSET = 'UTF-8'
    SUBJECT = 'Dining Suggestion'
    client = boto3.client('ses', region_name=REGION)
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    EMAIL_RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': message,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=EMAIL_SENDER,
        )
    except ClientError as err:
        logging.error(err)
    else:
        logging.error(f"Email sent! Message ID: {response['MessageId']}")


def clear_SQS(ReceiptHandle):
    logging.error("Clearing messages from Queue")
    client = boto3.client('sqs')
    client.delete_message(
        QueueUrl=QUEUE_URL,
        ReceiptHandle=ReceiptHandle
    )


def format_message(recommendations, preferences):
    if not len(recommendations):
        return "Sorry, there is not enough information about such restaurants in the database :("
    message = f"""Here are my suggestions for {preferences['Cuisine']} restaurants
    for {preferences['NumPeople']} people in {preferences['Location']}
    at {preferences['Time']} on {preferences['Date']}:\n
    """
    for idx, restaurant_info in enumerate(recommendations):
        message += f"{idx + 1}. {restaurant_info['name']} at {restaurant_info['address']}\n"
    message += "Have a nice dinner!"
    return message


def dispatch(event):
    logging.info(f'event is {event}')
    pre = poll_SQS()
    logging.info(f'from sqs {pre}')
    
    try:
        preferences = json.loads(event['Records'][0]['body'])
    except Exception:
        return "No Input"
    ReceiptHandle = event['Records'][0]['receiptHandle']
    clear_SQS(ReceiptHandle)

    recommendations = get_restaurant_recommendation(preferences['Cuisine'], preferences['Location'])
    message = format_message(recommendations, preferences)

    # either send SMS or Email
    logging.info(f'message is {message}')
    # send_SMS(preferences['PhoneNum'], message)
    send_SES(preferences['EmailAddress'], message)
    return message


def lambda_handler(event, context):
    message = dispatch(event)
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
        },
        'body': json.dumps({'results': message})
    }
