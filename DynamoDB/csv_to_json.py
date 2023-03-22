'''
helper script to manipulate the data to insert
'''
import csv
import json


# reference: https://www.maptrove.com/maps-for-business/zip-codes/new-york/new-york-city-new-york-zip-codes.html
def zip_to_region(zip_code):
    try:
        zip_code = int(zip_code)
        if zip_code >= 10001 and zip_code <= 10282:
            return 'Manhattan'
        elif zip_code >= 10301 and zip_code <= 10314:
            return 'Staten Island'
        elif zip_code >= 10451 and zip_code <= 10475:
            return 'Bronx'
        elif ((zip_code >= 11004 and zip_code <= 11109) or 
            (zip_code >= 11351 and zip_code <= 11697)):
            return 'Queens'
        elif (zip_code >= 11201 and zip_code <= 11256):
            return 'Brooklyn'
        else:
            return ''
    except:
        return ''


def make_json(csv_file_path, json_file_path, Cuisine):
    data = []

    with open(csv_file_path, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for rows in csvReader:
            # add column Cuisine
            rows['Cuisine'] = Cuisine
            # add column City_Region
            rows['City_Region'] = zip_to_region(rows['Zip_Code'])

            data.append(rows)
            
    with open(json_file_path, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))


if __name__ == '__main__':
    params = [
        ('yelp_chinese.csv', 'yelp_chinese.json', 'Chinese'),
        ('yelp_indian.csv', 'yelp_indian.json', 'Indian'),
        ('yelp_italian.csv', 'yelp_italian.json', 'Italian'),
        ('yelp_japanese.csv', 'yelp_japanese.json', 'Japanese'),
        ('yelp_korean.csv', 'yelp_korean.json', 'Korean'),
        ('yelp_mexican.csv', 'yelp_mexican.json', 'Mexican')
    ]

    for csv_file_path, json_file_path, Cuisine in params:
        make_json(csv_file_path, json_file_path, Cuisine)
