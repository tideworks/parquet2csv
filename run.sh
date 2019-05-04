#!/bin/bash

HOME=${PWD}
HADOOP_HOME=${PWD}

exec -a prq2csv java -ea -Dapp.name=prq2csv -jar target/parquet2csv-1.0-SNAPSHOT.jar "$@"
