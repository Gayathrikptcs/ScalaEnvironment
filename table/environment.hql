set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


CREATE EXTERNAL TABLE temperature (
year INT,
month INT,
day INT,
morning_observation DOUBLE,
noon_observation DOUBLE,
evening_observation DOUBLE,
tmax DOUBLE,
tmin DOUBLE,
tmean DOUBLE,
)
PARTITIONED BY (year int,month int,day int)
STORED AS PARQUET 
LOCATION '/user/environment/temperature';
