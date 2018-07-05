#!/bin/bash

export HOME=${PWD}
export HADOOP_HOME=${PWD}

java -ea -jar target/parquet2csv-1.0-SNAPSHOT.jar "$@"
