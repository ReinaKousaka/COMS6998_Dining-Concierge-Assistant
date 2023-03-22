''' Documentation:
DynamoDB: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
'''
import json
import boto3
from botocore.exceptions import ClientError


def insert_data(data_list, table='yelp-restaruants'):
    db = boto3.resource('dynamodb')
    table = db.Table(table)
    # overwrite if the same index is provided
    for data in data_list:
        response = table.put_item(
            TableName=table,
            Item=data
        )
    print('@insert_data: response', response)
    return response


def lambda_handler(event, context):
    file_paths = [
        'yelp_chinese.json', 'yelp_indian.json', 'yelp_italian.json',
        'yelp_japanese.json', 'yelp_korean.json', 'yelp_mexican.json'
    ]
    for json_file_path in file_paths:
        file = open(json_file_path)
        insert_data(json.loads(file))
