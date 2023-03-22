import csv
import json

IDs = set()

def process(file_path):
    with open(file_path) as file:
        json_data = json.load(file)

    # append
    with open('opensearch_data.json', 'a') as dest:
        for item in json_data:
            Business_ID = item['Business_ID']
            if Business_ID in IDs: continue
            IDs.add(Business_ID)
            dest.write(json.dumps({
                'index': {
                    '_index': 'restaurants',
                    '_id': len(IDs)
                }
            }))
            dest.write("\n")
            dest.write(json.dumps({
                'restaurants': Business_ID,
                'Cuisine': item['Cuisine']
            }))
            dest.write("\n")


if __name__ == '__main__':
    files = ['yelp_chinese.json', 'yelp_indian.json', 'yelp_italian.json',
        'yelp_japanese.json', 'yelp_korean.json', 'yelp_mexican.json']
    for file in files:
        process(file)
