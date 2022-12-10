# VINF project

- parsing wikipedia
- Vyparsovanie automobilových vozidiel z wikipédii a vytvorenie služby, ktorá by podľa zadaného automobilu (podľa jeho kategórii (automobilka, rok výroby, produkcia, trieda, predchodca, typ karosérie) umožňovala vyhľadať podobné automobilové vozidlá danej kategórie

## Wiki dataset source

- https://dumps.wikimedia.org/enwiki/20220920/

## Web predmetu

- https://vi2022.ui.sav.sk/doku.php?id=start

## Install XML jar
```
curl -O https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.15.0/spark-xml_2.12-0.15.0.jar
```

## Run pyspark wiki parsing

```
spark-submit --jars /Users/oliverkubicka/.local/share/virtualenvs/parsing_wiki-cx4OeFqi/lib/python3.9/site-packages/pyspark/jars/spark-xml_2.12-0.15.0.jar pyspark_parser.py
```

## Parsing wiki results

- without parallelization: 11620 s
- with parallelization:
  - (1 producer, 1 consumer): 9652 s
  - (4 producers, 1 consumer): 8061 s
  - pyspark: 5208 s
