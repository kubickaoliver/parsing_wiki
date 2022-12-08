import json
import os
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

# key=id value=vehicles_parameters
vehicles_index = {}

# parameters indexes have key=parameter value=vehicles_ids_list
manufacturer_index = {}
class_index = {}
layout_index = {}
production_index = {}
related_vehicles_index = {}


def find_simular_vehicle(vehicles: list, users_vehicle: dict) -> None:
    simular_cnt = 0
    simular_vehicles = []
    for vehicle in vehicles:
        cand_cnt = 0
        if vehicle['manufacturer'] != users_vehicle['manufacturer']:
            for file_v in vehicle['manufacturer']:
                for user_v in users_vehicle['manufacturer']:
                    if file_v == user_v:
                        cand_cnt += 1
            
            for file_v in vehicle['class']:
                for user_v in users_vehicle['class']:
                    if file_v == user_v:
                        cand_cnt += 1

        
        for file_v in vehicle['layout']:
            for user_v in users_vehicle['layout']:
                if file_v == user_v:
                    cand_cnt += 1
    
        if vehicle['production_year'] == users_vehicle['production_year']:
            cand_cnt += 1
        
        if cand_cnt >= simular_cnt and cand_cnt != 0:
            simular_cnt = cand_cnt
            simular_vehicles.append({'simular_cnt': simular_cnt, 'vehicle': vehicle})

    print('Your vehicle parameters:')
    print(users_vehicle, end='\n\n')
    print('Simular vehicles:')
    print(simular_vehicles[-1])


def create_parameter_index(vehicle_parameters: list, vehicle_id: int, parameter_index: dict) -> dict:
    for parameter in vehicle_parameters:
        if type(parameter) != int:
            parameter = wnl.lemmatize(parameter)
        index_value_list = parameter_index.get(parameter, None)
        if index_value_list == None:
            parameter_index[parameter] = [vehicle_id]
        else:
            index_value_list.append(vehicle_id)


def create_indexes(vehicles: list) -> None:
    global vehicles_index, manufacturer_index, class_index, layout_index, production_index, related_vehicles_index
    
    # to co je za zatvorkou vymazat alebo to čo je v zatvorke vyextrahovat
    for i, vehicle in enumerate(vehicles):
        vehicles_index[i] = vehicle
        create_parameter_index(vehicle_parameters=vehicle['manufacturer'], vehicle_id=i, parameter_index=manufacturer_index)
        create_parameter_index(vehicle_parameters=vehicle['class'], vehicle_id=i, parameter_index=class_index)
        create_parameter_index(vehicle_parameters=vehicle['layout'], vehicle_id=i, parameter_index=layout_index)
        create_parameter_index(vehicle_parameters=vehicle['related'], vehicle_id=i, parameter_index=related_vehicles_index)
        create_parameter_index(vehicle_parameters=[vehicle['production_year']], vehicle_id=i, parameter_index=production_index)


def query_user_vehicle(users_input: str) -> list:
    users_vehicle = {}
    for key, item in vehicles_index.items():
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


def query_simular_vehicle(
    users_vehicle_key: int,
    manufacturer: list,
    vehicle_class: list,
    layout: list,
    production_year: int,
    related_vehicle: list
) -> dict:
    global vehicles_index, manufacturer_index, class_index, layout_index, production_index, related_vehicles_index
    vehicles_ids = []
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=manufacturer, parameter_index=manufacturer_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=vehicle_class, parameter_index=class_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=layout, parameter_index=layout_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=[production_year], parameter_index=production_index)
    vehicles_ids = find_vehicle_ids(vehicles_ids=vehicles_ids, users_vehicle_parameters=related_vehicle, parameter_index=related_vehicles_index)
    
    vehicles_ids = list(filter((users_vehicle_key).__ne__, vehicles_ids))
    most_common_id = max(set(vehicles_ids), key=vehicles_ids.count)

    return vehicles_index.get(most_common_id, None)


if __name__ == "__main__":
    # audi e-tron gt
    print('Enter the name of the vehicle:', end=' ')
    input_str = input().lower()
    json_file = open('vehicles_multiprocessing.json')
    vehicles = json.load(json_file)
    
    create_indexes(vehicles)

    key, user_vehicle = query_user_vehicle(input_str)
    
    if user_vehicle != {}:
        simular_vehicle = query_simular_vehicle(
            users_vehicle_key=key,
            manufacturer=user_vehicle['manufacturer'],
            vehicle_class=user_vehicle['class'],
            layout=user_vehicle['layout'],
            production_year=user_vehicle['production_year'],
            related_vehicle=user_vehicle['related']
        )
        print('')
        print('Most simular vehicle is:')
        print(simular_vehicle)
    else:
        print('We are not able to find your vehicle in the wiki :(')
