from collections import Counter
import nltk
from nltk.stem import WordNetLemmatizer
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('omw-1.4')
wnl = WordNetLemmatizer()

class Index:
    def __init__(self):
        # key=id value=vehicles_parameters
        self.vehicles_index = {}
        # parameters indexes have key=parameter value=vehicles_ids_list
        self.manufacturer_index = {}
        self.class_index = {}
        self.layout_index = {}
        self.production_index = {}
        self.related_vehicles_index = {}


def create_parameter_index(vehicle_parameters: list, vehicle_id: int, parameter_index: dict) -> dict:
    for parameter in vehicle_parameters:
        if type(parameter) != int:
            parameter = wnl.lemmatize(parameter)
        index_value_list = parameter_index.get(parameter, None)
        if index_value_list == None:
            parameter_index[parameter] = [vehicle_id]
        else:
            parameter_index[parameter].append(vehicle_id)


def create_indexes(vehicles: list, index: Index):
    for i, vehicle in enumerate(vehicles):
        index.vehicles_index[i] = vehicle
        create_parameter_index(vehicle_parameters=vehicle['manufacturer'], vehicle_id=i, parameter_index=index.manufacturer_index)
        create_parameter_index(vehicle_parameters=vehicle['class'], vehicle_id=i, parameter_index=index.class_index)
        create_parameter_index(vehicle_parameters=vehicle['layout'], vehicle_id=i, parameter_index=index.layout_index)
        create_parameter_index(vehicle_parameters=vehicle['related'], vehicle_id=i, parameter_index=index.related_vehicles_index)
        create_parameter_index(vehicle_parameters=[vehicle['production_year']], vehicle_id=i, parameter_index=index.production_index)
    return index


def query_user_vehicle(users_input: str, index: Index) -> list:
    users_vehicle = {}
    for key, item in index.vehicles_index.items():
        if users_input == item['name']:
            return key, item
        elif users_input in item['name'] or item['name'] in users_input:
            users_vehicle = item
    return users_vehicle


def find_vehicle_ids(vehicles_ids: list, users_vehicle_parameters: list, parameter_index: dict) -> list:
    for item in users_vehicle_parameters:
        ids = parameter_index.get(item, None)
        if ids:
            vehicles_ids = vehicles_ids + ids
    return vehicles_ids


def query_three_simular_vehicle(
    users_vehicle_key: int,
    manufacturer: list,
    vehicle_class: list,
    layout: list,
    production_year: int,
    related_vehicle: str,
    index: Index
) -> dict:
    vehicles_ids = []
    similar_vehicles = []
    # if related vehicle is present we can find it in related_index
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=related_vehicle, parameter_index=index.related_vehicles_index)
    for vehicle_id in vehicles_ids:
        rel_vehicle = index.vehicles_index.get(vehicle_id, None)
        if rel_vehicle:
            similar_vehicles.append(rel_vehicle)

    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=manufacturer, parameter_index=index.manufacturer_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=vehicle_class, parameter_index=index.class_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=layout, parameter_index=index.layout_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=[production_year], parameter_index=index.production_index)

    # Remove users vehicle ids
    vehicles_ids = list(filter((users_vehicle_key).__ne__, vehicles_ids))
    
    # Find three common ids
    counter = Counter(vehicles_ids)
    three_most_common_ids = counter.most_common(3)
    
    for i in three_most_common_ids:
        vehicle = index.vehicles_index.get(i[0], None)
        if vehicle and vehicle not in similar_vehicles:
            similar_vehicles.append(index.vehicles_index.get(i[0], None))
    return similar_vehicles
