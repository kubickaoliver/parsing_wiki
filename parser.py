import json
import bz2
import re
import time

vehicles = []

class Vehicle:
    def __init__(self, name='', manufacturer=[], production='', vehicle_class=[], layout=[], related=[]):
        self.name = name
        self.manufacturer = manufacturer
        self.production = production
        self.vehicle_class = vehicle_class
        self.layout = layout
        self.related = related

def parse_parameter_value(parameter: str, text: str) -> str:
    pattern = f"(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(\s)*"
    value = re.sub(pattern, '', text)
    value = re.split('[&]', value)
    return value[0].replace('[', '').replace(']', '').replace('{', '').replace('}', '').replace('\n', '').rstrip()


def find_parameter(parameter: str, text: str):
    pattern = f"(\|){{1}}(\s)*({parameter}){{1}}(\s)*(=){{1}}(\s)*"
    return re.search(pattern, text)


def get_vehicle_parameters(vehicles: list, text: str):
    text = text.lower()
    if vehicles[-1]['name'] == '' and find_parameter('name', text):
        vehicles[-1]['name'] = parse_parameter_value('name', text)
    elif vehicles[-1]['manufacturer'] == [] and find_parameter('manufacturer', text):
        manufacturers = parse_parameter_value('manufacturer', text)
        manufacturers = re.split('[|]', manufacturers)
        vehicles[-1]['manufacturer'] = [str(item).lstrip().rstrip() for item in manufacturers]
    elif vehicles[-1]['class'] == [] and find_parameter('class', text):
        vehicle_class = parse_parameter_value('class', text)
        vehicle_class = re.split('[|/]', vehicle_class)
        vehicles[-1]['class'] = [str(item).lstrip().rstrip() for item in vehicle_class]
    elif vehicles[-1]['production_year'] == '' and find_parameter('production', text):
        years = parse_parameter_value('production', text)
        years = re.findall(r'([1-2][0-9]{3})', years)
        if years:
            vehicles[-1]['production_year'] = int(years[0])
    elif vehicles[-1]['layout'] == [] and find_parameter('layout', text):
        layout = parse_parameter_value('layout', text)
        layout = re.split('[|/,#]', layout)
        vehicles[-1]['layout'] = [str(item).lstrip().rstrip() for item in layout]
    elif vehicles[-1]['related'] == [] and find_parameter('related', text):
        related = parse_parameter_value('related', text)
        related = re.split('[|/,#]', related)
        vehicles[-1]['related'] = [str(item).lstrip().rstrip() for item in related]


if __name__ == "__main__":
    # dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2
    # dataset/enwiki-20220920-pages-meta-current.xml.bz2
    start = time.time()
    with bz2.BZ2File('dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2') as f:
        flag = -1
        for row in f:
            row = row.decode("utf-8")
            row = row.lstrip().rstrip()
            if '{{Infobox automobile' == row:
                flag = 0
                vehicles.append({ 'name': '', 'manufacturer': [], 'class': [], 'layout': [], 'production_year': '', 'related': [] })
                continue
            elif ('}}' == row or '|}' == row) and flag == 0:
                flag = 1
            elif flag == 1 and row != '' and row[0] == '|':
                flag = 0
            elif flag == 0:
                get_vehicle_parameters(vehicles=vehicles, text=row)
        f.close()
    
    with open('vehicles.json', 'w') as json_file:
        json.dump(vehicles, json_file, indent=2)
        json_file.close()
    
    print(time.time() - start, 's')