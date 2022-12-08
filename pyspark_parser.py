import json
import bz2
import re
import time
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

vehicles = []
flag = -1


class Vehicle:
    def __init__(self, name='', manufacturer=[], production='', vehicle_class=[], layout=[], related=[]):
        self.name = name
        self.manufacturer = manufacturer
        self.production = production
        self.vehicle_class = vehicle_class
        self.layout = layout
        self.related = related


def parse_parameter_value(parameter: str, text: str) -> str:
    """ regex = f"(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(\s)*"
    re.findall(regex, text, re.MULTILINE)
    print() """
    regex = f"(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(.*?)(?=\\\\n)"
    matches = re.findall(regex, text, re.MULTILINE)

    if matches:
        value = matches[0][5]
        value = re.split('<|>|[|&|]', value)
        return value[0].replace('[', '').replace(']', '').replace('{', '').replace('}', '').replace('\n', '').rstrip()
    return None


""" def find_parameter(parameter: str, text: str):
    pattern = f"(?<=)(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(\s)*(.)*?(?=\\n)"
    return re.search(pattern, text) """


def get_vehicle_parameters(text: str):
    vehicle = { 'name': '', 'manufacturer': [], 'class': [], 'layout': [], 'production_year': '', 'related': [] }
    text = text.lower()
    
    value = parse_parameter_value('name', text)
    if value:
        vehicle['name'] = value

    value = parse_parameter_value('manufacturer', text)
    if value:
        manufacturers = re.split('[|]', value)
        vehicle['manufacturer'] = [str(item).lstrip().rstrip() for item in manufacturers]
    
    value = parse_parameter_value('class', text)
    if value:
        vehicle_class = re.split('(|)|[|/|]', value)
        vehicle['class'] = [str(item).lstrip().rstrip() for item in vehicle_class]

    value = parse_parameter_value('production', text)
    if value:
        years = re.findall(r'([1-2][0-9]{3})', value)
        if years:
            vehicle['production_year'] = int(years[0])
    
    value = parse_parameter_value('layout', text)
    if value:
        layout = re.split('[|/|,|#|]', value)
        vehicle['layout'] = [str(item).lstrip().rstrip() for item in layout]

    value = parse_parameter_value('related', text)
    if value:
        related = re.split('[|/|,|#]', value)
        vehicle['related'] = [str(item).lstrip().rstrip() for item in related]
    
    return vehicle


def parsing_wiki(row):
    regex = r"(\\n)(\s)*({\{){1}(\s)*(Infobox){1}(\s)*(automobile){1}(.*)(?=(\\n\}\}\\n)|(\\n\|\}\\n))"
    matches = re.findall(regex, str(row), re.MULTILINE)
    if matches:
        return get_vehicle_parameters(text=matches[0][7])
    return None


if __name__ == "__main__":
    sc = SparkContext("local[12]", "vehicle_wiki_parsing")
    spark = SparkSession.builder.master("local[12]").appName("vehicle_wiki_parsing").getOrCreate()
    start = time.time()
    # dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2
    # dataset/enwiki-20220920-pages-meta-current.xml.bz2
    
    xml_schema = StructType([ \
        StructField('revision', StructType([
            StructField('text', StringType(), True)
        ]))
    ])

    df = spark.read\
        .format('xml')\
            .options(rowTag="page")\
                .load("dataset/enwiki-20220920-pages-meta-current.xml.bz2", schema=xml_schema)

    rdd2 = df.rdd.map(lambda row: parsing_wiki(row)).filter(lambda row: row != None)
    rdd2.saveAsTextFile("./parsed_vehicles")
    print(time.time() - start, 's')
    
    """ with open('vehicles.json', 'w') as json_file:
        json.dump(vehicles, json_file, indent=2)
        json_file.close() """
 