import bz2


class Automobil:
    def __init__(self, name='', manufacturer='', production='', car_class='', layout=''):
        self.name = name
        self.manufacturer = manufacturer
        self.production = production
        self.car_class = car_class
        self.layout = layout


if __name__ == "__main__":
    with bz2.BZ2File('dataset/enwiki-20220920-pages-meta-current10.xml-p4045403p5399366.bz2') as f:
        automobiles = []
        flag = False
        for row in f:
            row = row.decode("utf-8")
            if '{{Infobox automobile' == row.lstrip().rstrip():
                flag = True
                automobiles.append(Automobil())
                continue
            elif '}}' == row.rstrip().lstrip() and flag == True:
                flag = False
            if flag:
                if '| name' in row:
                    automobiles[-1].name = row.replace('| name', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '|name' in row:
                    automobiles[-1].name = row.replace('|name', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '| manufacturer' in row:
                    automobiles[-1].manufacturer = row.replace('| manufacturer', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '|manufacturer' in row:
                    automobiles[-1].manufacturer = row.replace('|manufacturer', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '| class' in row:
                    automobiles[-1].car_class = row.replace('| class', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '|class' in row:
                    automobiles[-1].car_class = row.replace('|class', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '| production' in row:
                    automobiles[-1].production = row.replace('| production', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '|production' in row:
                    automobiles[-1].production = row.replace('|production', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '| layout' in row:
                    automobiles[-1].layout = row.replace('| layout', '').replace('=', '').replace('\n', '').lstrip().rstrip()
                elif '|layout' in row:
                    automobiles[-1].layout = row.replace('|layout', '').replace('=', '').replace('\n', '').lstrip().rstrip()
        
        for car in automobiles:
            print({
                'name': car.name,
                'manufacturer': car.manufacturer,
                'class': car.car_class,
                'layout': car.layout,
                'production': car.production
            })
        print(len(automobiles))
        f.close()
