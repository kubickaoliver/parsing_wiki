import re

# to co ma dlzku menej ako tri to ani neratat
value = '[[front-engine, front-wheel drive layout|front-engine, front-wheel drive]]'
value_1 = '[[hudson motor car company]] <br/> [[american motors|american motors corporation]]'
value_2 = '1935–48'
value_3 = ' [[front-engine, rear-wheel-drive layout|front-engine, rear-wheel-drive]]'
value_4 = '1947&ndash;1950<br/>487 {units}'
value_5 = "general motors-holden\\'s"
vehicle_class = re.split(r"[/;,.#()\[\]|<>{}\\']\s*", value_4)
print(vehicle_class)