#!/bin/bash

if [[ "$OSTYPE" == 'msys' ]]; then
   sep=';'
else
   sep=':'
fi

export HOME=${PWD}
export HADOOP_HOME=${PWD}
dir=./target
clspath=`find ${dir}/ -type f -name "*.jar"`
clspath=`echo ${clspath}|tr ' ' $sep`
cls=com/tideworks/data_load/DataLoad
#clsldr=com.tideworks.data_load.DataLoad.CustomClassLoader
clsldr=com.tideworks.data_load.CustomClassLoader

java -ea -cp "avro-classes${sep}${clspath}" -Djava.system.class.loader=${clsldr} ${cls} $*
