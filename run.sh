#!/bin/bash

HOME=${PWD}
HADOOP_HOME=${PWD}

exec java -ea -jar target/parquet2csv-1.0-SNAPSHOT.jar "$@"
