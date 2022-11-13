import json


if __name__ == "__main__":
    # audi e-tron gt
    print('Enter the name of the vehicle:')
    input_str = input().lower()
    json_file = open('vehicles.json')
    vehicles = json.load(json_file)
    
    # Find user's vehicle
    users_vehicle = {}
    for vehicle in vehicles:
        if input_str == vehicle['name']:
            users_vehicle = vehicle
            break
    
    # Find the most similar vehicle to user's vehicle
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
    print(users_vehicle)
    print('Simular vehicles:')
    print(simular_vehicles[-1])


        
