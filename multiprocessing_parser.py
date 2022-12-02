from multiprocessing import Process, Queue
import time
import bz2
import re
import json


LARGE_FILE_SIZE_BYTES = 194477488262
SMALL_FILE_SIZE_BYTES = 3784455896


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


def producer(infoboxes_queue: Queue, starting_file_pos, ending_file_pos):
    with bz2.BZ2File('dataset/enwiki-20220920-pages-meta-current.xml.bz2') as f:
        f.seek(starting_file_pos)
        flag = -1
        infobox_rows = []
        for row in f:
            row = row.decode("utf-8")
            row = row.lstrip().rstrip()
            if '{{Infobox automobile' in row:
                flag = 0
                infoboxes_queue.put(infobox_rows)
                infobox_rows = []
                continue
            elif ('}}' == row or '|}' == row) and flag == 0:
                flag = 1
            elif flag == 1 and row != '' and row[0] == '|':
                flag = 0
            elif flag == 0:
                infobox_rows.append(row)
            if f.tell() >= ending_file_pos:
                break
        f.close()
    print(f'Producer with starting position {starting_file_pos} end...')
    if ending_file_pos == LARGE_FILE_SIZE_BYTES:
        infoboxes_queue.put(-1)


def consumer(infoboxes_queue: Queue):
    parsed_vehicles = []
    while 1:
        try:
            infobox_rows = infoboxes_queue.get()
        except:
            pass
        else:
            if infobox_rows == -1:
                print('consumer end...')
                break
            parsed_vehicles.append({ 'name': '', 'manufacturer': [], 'class': [], 'layout': [], 'production_year': '', 'related': [] })
            for row in infobox_rows:
                get_vehicle_parameters(vehicles=parsed_vehicles, text=row)
        time.sleep(0.01)
    print(len(parsed_vehicles))
    with open('vehicles_multiprocessing.json', 'w') as json_file:
        json.dump(parsed_vehicles, json_file, indent=2)
        json_file.close()


if __name__ == "__main__":
    infoboxes_queue = Queue()
    
    chunk_size = int(LARGE_FILE_SIZE_BYTES/4)

    start = time.time()

    firsr_p_producer = Process(target=producer, args=(infoboxes_queue, 0, chunk_size - 1))
    firsr_p_producer.start()

    second_p_producer = Process(target=producer, args=(infoboxes_queue, chunk_size, chunk_size * 2 - 1))
    second_p_producer.start()

    third_p_producer = Process(target=producer, args=(infoboxes_queue, chunk_size * 2, chunk_size * 3 - 1))
    third_p_producer.start()

    fourth_p_producer = Process(target=producer, args=(infoboxes_queue, chunk_size * 3, LARGE_FILE_SIZE_BYTES))
    fourth_p_producer.start()
    
    p_consumer = Process(target=consumer, args=(infoboxes_queue,))
    p_consumer.start()

    firsr_p_producer.join()
    second_p_producer.join()
    third_p_producer.join()
    fourth_p_producer.join()
    p_consumer.join()
    
    print(time.time() - start, 's')
    infoboxes_queue.close()