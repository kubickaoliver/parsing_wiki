import re
import os
import json
import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from search_simular_vehicle import create_indexes, query_user_vehicle, query_three_simular_vehicle, Index


# Do you want to parse dataset? 
PARSE_DATASET = False

# If parse the data == true choose dataset name
# Small dataset - dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2
# Big dataset - dataset/enwiki-20220920-pages-meta-current.xml.bz2
DATASET_PATH = 'dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2'

# This variable have to be set in case that you don't want to parse data (PARSE_DATASET == False) but just searching in parsed data
# If you don't want to parse datset, choose dataset folder name in which you want to search for simular vehicles
# You want big dataset, you have to choose  './parsed_vehicles_big_dataset'
# You want small dataset, you have to choose  './parsed_vehicles_small_dataset'
PARSED_VEHICLES = './parsed_vehicles_big_dataset'

# For big dataset choose for example audi e-tron gt
# For small dataset choose for example ford s-max
USERS_VEHICLE = 'audi e-tron gt'


class Vehicle:
    def __init__(self, name='', manufacturer=[], production='', vehicle_class=[], layout=[], related=[]):
        self.name = name
        self.manufacturer = manufacturer
        self.production = production
        self.vehicle_class = vehicle_class
        self.layout = layout
        self.related = related

# General function for parsing parameters values from infobox
def parse_parameter_value(parameter: str, text: str) -> list:
    regex = f"(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(.*?)(?=\\\\n)"
    matches = re.findall(regex, text, re.MULTILINE)

    if matches:
        value = matches[0][5]
        # Split parameter's value accroding to speciefied delimiters
        values = re.split(r"[/;,.#()\[\]|<>{}\\']\s*", value)
        result_values = []
        for item in values:
            item = str(item).lstrip().rstrip().replace('\n', '').replace('"', '').replace("'", "")
            # Remove words, which do not make sense
            if item != '' and len(item) > 2:
                result_values.append(item)
        return result_values
    return None

# Parsing from infobox all paramers defined in Vehicle class
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
                # Parse first year from string
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

# Find and parse infobox
def parsing_wiki(row):
    regex = r"(\\n)(\s)*({\{){1}(\s)*(Infobox){1}(\s)*(automobile){1}(.*)(?=(\\n\}\}\\n)|(\\n\|\}\\n))"
    matches = re.findall(regex, str(row), re.MULTILINE)
    if matches:
        # Parse infobox
        return get_vehicle_parameters(text=matches[0][7])
    return None

# Create and search simular vehicles in all indexes 
def search_simular_vehicle(parsed_vehicle_folder: str):
    vehicles = []
    # Create index from parsed vehicles
    os.chdir(parsed_vehicle_folder)
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

    # Find users vehicle in vehicle index
    key, user_vehicle = query_user_vehicle(USERS_VEHICLE, vehicle_index)
    print('')
    print("Your vehicle parameters:")
    print(user_vehicle)
    similar_vehicles = None
    if user_vehicle != {}:
        # If users vehicle is present in vehicle index find 3 most simular vehicles
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
    return similar_vehicles


if __name__ == "__main__":
    parsed_vehicle_folder = './new_parsed_vehicles'
    if PARSE_DATASET == True:
        sc = SparkContext("local[*]", "wiki_parsing")
        spark = SparkSession.builder.master("local[*]").appName("wiki_parsing").getOrCreate()
        start = time.time()
        
        # Create xml schema for pyspark
        xml_schema = StructType([\
            StructField('revision', StructType([
                StructField('text', StringType(), True)
            ]))
        ])

        # Read pages within wiki file based on xml schema
        df = spark.read\
            .format('xml')\
                .options(rowTag="page")\
                    .load(DATASET_PATH, schema=xml_schema)

        # Parse wiki pages and filter out result that are None
        rdd2 = df.rdd.map(lambda row: parsing_wiki(row)).filter(lambda row: row != None)

        #??Save parsed dict to specified folder
        rdd2.saveAsTextFile(parsed_vehicle_folder)
        print(time.time() - start, 's')
    else:
        parsed_vehicle_folder = PARSED_VEHICLES

    # Find 3 most simular vehicles to user's vehicle
    search_simular_vehicle(parsed_vehicle_folder)
 