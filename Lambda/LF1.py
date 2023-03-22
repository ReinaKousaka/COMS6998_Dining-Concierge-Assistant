''' Documentation (we use Lex V1 instead of V2): 
Lambda with Lex: https://docs.aws.amazon.com/lex/latest/dg/lambda-input-response-format.html
'''

import os
import json
import logging
import dateutil.parser
import datetime
import time
import boto3

QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/605752467729/chatbot-Q1'
# message Simple Queue Service
def msg_sqs(preferences):
    sqs = boto3.client('sqs')
    try:
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody = json.dumps(preferences)
        )
    except Exception as err:
        logging.error(err)


def delegate(slots):
    return {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots,
        },
    }

def close(message=''):
    return {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': 'Fulfilled',
            'message': {
                'contentType': 'PlainText',
                'content': message
            }
        },
    }

def elicit_intent(message):
    return {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'ElicitIntent',
            'message': {
                'contentType': 'PlainText',
                'content': message,
            },
        },
    }
    

def elicit_slot(slots, intent, slotToElicit, message=""):
    return {
        'sessionAttributes': sessionAttributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'slotToElicit': slotToElicit,
            'intentName': intent,
            'slots': slots,
            'message': {
                'contentType': 'PlainText',
                'content': message,
            }
        },
    }


def dining_suggestions_handler(event):
    slots = event['currentIntent']['slots']
    intent = event['currentIntent']['name']
    
    cuisine_types = ['chinese', 'japanese', 'korean', 'indian', 'mexican', 'italian']
    city_region_types = ['manhattan', 'queens', 'brooklyn', 'bronx', 'staten island']
    slot_names = ['Cuisine', 'NumPeople', 'Location', 'Date', 'Time', 'PhoneNum', 'EmailAddress']
    preferences = dict.fromkeys(slot_names)

    def _validate_slot(slot_name):
        slot_value = slots.get(slot_name)
        if slot_name == 'Cuisine':
            if slot_value.lower() not in cuisine_types:
                return f'Please select cuisine from: {", ".join(cuisine_types)}'
        elif slot_name == 'NumPeople':
            try:
                if (int(slot_value) < 1 or
                    int(slot_value) > 15
                ):
                    return 'Please enter a valid number of people (1 ~ 15)'
            except:
                return 'Please enter a valid number of people (1 ~ 15)'
        elif slot_name == 'Location':
            if slot_value.lower() not in city_region_types:
                return f'Please select location from: {", ".join(city_region_types)}'
        elif slot_name == 'Date':
            try:
                parsed_date = dateutil.parser.parse(slots.get('Date')).date()
                if parsed_date < datetime.date.today():
                    return 'Please enter a correct date'
            except Exception as err:
                logging.error(err)
                return 'Please re-enter the date'
        elif slot_name == 'Time':
            try:
                parsed_date = dateutil.parser.parse(slots.get('Date')).date()
                parsed_time = dateutil.parser.parse(slots.get('Time')).timestamp()
                if parsed_date == datetime.date.today() and parsed_time < datetime.datetime.now().timestamp():
                    return 'Please enter a correct time'
            except Exception as err:
                logging.error(err)
                return 'Please re-enter the time'
        elif slot_name == 'PhoneNum':
            if len(slot_value) != 10 or not slot_value.isdigit():
                return 'Please enter correct phone number'
        return None


    for slot_name in slot_names:
        if slots.get(slot_name) is None:
            return delegate(slots)
        elif (res := _validate_slot(slot_name)) is not None:
            # invalid slots -->  ElicitSlot (elicit a value from the user)
            return elicit_slot(slots, intent, slot_name, res)
        else:
            preferences[slot_name] = slots.get(slot_name)
    
    msg_sqs(preferences)
    return elicit_intent(message='You are all set!')


def dispatch(event):
    intent = event['currentIntent']['name']
   
    global sessionAttributes
    if event['sessionAttributes'] is not None:
        sessionAttributes = event['sessionAttributes']
    else:
        sessionAttributes = {}

    if intent == 'GreetingIntent':
        return elicit_intent(message='Hi there, how can I help you?')
    elif intent == 'ThankYouIntent':
        return close(message='You are welcome! Have a nice dinner!')
    elif intent == 'DiningSuggestionsIntent':
        return dining_suggestions_handler(event)


# the main handler runs when the function is invoked
def lambda_handler(event, context):
    response = dispatch(event)
    return response
