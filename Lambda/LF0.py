''' Documentation:
boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
post_text format: https://docs.aws.amazon.com/lex/latest/dg/API_runtime_PostText.html
'''
import boto3
import json
import logging

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    message = event['messages'][0]['unstructured']['text']   # user's input text

    response = client.post_text(
        botName='latias_dining_chatbot_vone',
        botAlias='chatbot_latias',
        userId='user0',
        inputText=message,
    )

    return {
        'statusCode': 200,
        'messages': [
            {
                "type": "unstructured", 
                "unstructured": {
                    "text": json.dumps(response['message'])
                }
            }
        ]
    }
