CREATE DATABASE db_task9;
CREATE TABLE db_task9.userlog (day Int32, ticktime Float64, speed Float64) ENGINE = Kafka('localhost:9092', 'task9_14', 'task9_grp', 'JSONEachRow');