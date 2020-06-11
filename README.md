# big-data-spark
Hands on experience working with sparkSQL, spark-Streaming, spark-Mllib & spark-graphX in Scala & Pyspark.

## How to run :

#### Scala :
- Import the project using build.sbt in IDE and run.
#### Pyspark :
- Create google colab account and run notebooks as it is after loading datasets (from src/main/resouces) into colab.

## Datasets :

#### NSE (National Stock Exchange) data :
- **Use** : Working with dataframe APIs and sparkSQL.
- It is small and is uploaded in this repo.

#### Event data :
- **Use** : For illustrating Complex JSON data-processing.
- It is small and is loaded in this repo.

#### NOAA (National Oceanic and Atmospheric Administration) data source :
- **Use** : For working with basic Machine-Learning. 
- Download <year>.csv from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/  and save it under src/main/resources/NOAA/<year>.csv
- Download gscnd-stations.txt from ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ and save it under src/main/resources/NOAA/ghcnd-stations.csv
- Update the climateData property accordingly in application.properties
  
#### Twitter credentials :
- **Use** : For spark-streaming illustrations.
- Request for twitter developer accounts at developer.twitter.com.
- Update the application.properties accordingly 
