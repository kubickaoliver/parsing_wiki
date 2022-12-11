import re
import os
import json
import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from search_simular_vehicle import create_indexes, query_user_vehicle, query_three_simular_vehicle, Index

# vehicle name for example: audi e-tron gt
USERS_VEHICLE = 'audi e-tron gt'

class Vehicle:
    def __init__(self, name='', manufacturer=[], production='', vehicle_class=[], layout=[], related=[]):
        self.name = name
        self.manufacturer = manufacturer
        self.production = production
        self.vehicle_class = vehicle_class
        self.layout = layout
        self.related = related


def parse_parameter_value(parameter: str, text: str) -> list:
    regex = f"(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(.*?)(?=\\\\n)"
    matches = re.findall(regex, text, re.MULTILINE)

    if matches:
        value = matches[0][5]
        values = re.split(r"[/;,.#()\[\]|<>{}\\']\s*", value)
        result_values = []
        for item in values:
            item = str(item).lstrip().rstrip().replace('\n', '').replace('"', '').replace("'", "")
            if item != '' and len(item) > 2:
                result_values.append(item)
        return result_values
    return None


def get_vehicle_parameters(text: str):
    vehicle = { 'name': '', 'manufacturer': [], 'class': [], 'layout': [], 'production_year': '', 'related': [] }
    text = text.lower()
    
    values = parse_parameter_value('name', text)
    if values:
        vehicle['name'] = values[0]

        values = parse_parameter_value('manufacturer', text)
        if values:
            vehicle['manufacturer'] = values
        
        values = parse_parameter_value('class', text)
        if values:
            vehicle['class'] = values

        values = parse_parameter_value('production', text)
        if values:
            for item in values:
                years = re.findall(r'([1-2][0-9]{3})', item)
                if years:
                    vehicle['production_year'] = int(years[0])
                    break
        
        values = parse_parameter_value('layout', text)
        if values:
            vehicle['layout'] = values

        values = parse_parameter_value('related', text)
        if values:
            vehicle['related'] = values
        
        return vehicle
    return None


def parsing_wiki(row):
    regex = r"(\\n)(\s)*({\{){1}(\s)*(Infobox){1}(\s)*(automobile){1}(.*)(?=(\\n\}\}\\n)|(\\n\|\}\\n))"
    matches = re.findall(regex, str(row), re.MULTILINE)
    if matches:
        return get_vehicle_parameters(text=matches[0][7])
    return None


def search_simular_vehicle():
    vehicles = []
    # Create index from parsed vehicles
    os.chdir('./parsed_vehicles_big_dataset')
    for file_name in os.listdir():
        if not file_name.endswith(".crc") and file_name != '_SUCCESS':
            with open(f'{file_name}', 'r') as f:
                for row in f:
                    if row:
                        row = row.replace('\n', '').replace("'", '"')
                        row = json.loads(row)
                        vehicles.append(row)
    
    vehicle_index = Index()
    vehicle_index = create_indexes(vehicles, vehicle_index)
    
    key, user_vehicle = query_user_vehicle(USERS_VEHICLE, vehicle_index)
    print('')
    print("Your vehicle parameters:")
    print(user_vehicle)

    if user_vehicle != {}:
        similar_vehicles = query_three_simular_vehicle(
            users_vehicle_key=key,
            manufacturer=user_vehicle['manufacturer'],
            vehicle_class=user_vehicle['class'],
            layout=user_vehicle['layout'],
            production_year=user_vehicle['production_year'],
            related_vehicle=[user_vehicle['name']],
            index=vehicle_index
        )
        print('')
        print('Most simular vehicles are:')
        for vehicle in similar_vehicles:
            print(vehicle)
    else:
        print('We are not able to find your vehicle in the wiki :(')


if __name__ == "__main__":
    sc = SparkContext("local[*]", "wiki_parsing")
    spark = SparkSession.builder.master("local[*]").appName("wiki_parsing").getOrCreate()
    start = time.time()
    
    xml_schema = StructType([\
        StructField('revision', StructType([
            StructField('text', StringType(), True)
        ]))
    ])

    # dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2
    # dataset/enwiki-20220920-pages-meta-current.xml.bz2
    df = spark.read\
        .format('xml')\
            .options(rowTag="page")\
                .load("dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2", schema=xml_schema)

    rdd2 = df.rdd.map(lambda row: parsing_wiki(row)).filter(lambda row: row != None)
    rdd2.saveAsTextFile("./parsed_vehicles")
    print(time.time() - start, 's')

    search_simular_vehicle()
 