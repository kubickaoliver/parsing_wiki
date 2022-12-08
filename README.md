# VINF project

- parsing wikipedia
- Vyparsovanie automobilových vozidiel z wikipédii a vytvorenie služby, ktorá by podľa zadaného automobilu (podľa jeho kategórii (automobilka, rok výroby, produkcia, trieda, predchodca, typ karosérie) umožňovala vyhľadať podobné automobilové vozidlá danej kategórie

## Dataset source

- https://dumps.wikimedia.org/enwiki/20220920/


## Web predmetu

- https://vi2022.ui.sav.sk/doku.php?id=start

## Run docker image (iisas/hadoop-spark-pig-hive:2.9.2)

```
docker pull iisas/hadoop-spark-pig-hive:2.9.2
```

```
docker run -it -p 50070:50070 -p 8088:8088 -p 8080:8080 iisas/hadoop-spark-pig-hive:2.9.2
```

## Parsing wiki

- without parallelization: 11620 s
- with parallelization:
  - (1 producer, 1 consumer): 9652 s
  - (4 producers, 1 consumer): 8061 s
  - pyspark: 5208 s
