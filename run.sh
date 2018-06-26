#!/bin/bash

if [[ "$OSTYPE" == 'msys' ]]; then
   sep=';'
else
   sep=':'
fi

export HOME=${PWD}
export HADOOP_HOME=${PWD}
dir=./target
clspath=`find ${dir}/ -maxdepth 1 -type f -name "*.jar"|tr '\n' $sep`
export CLASSPATH=${clspath}$sep${CLASSPATH}
cls=com/tideworks/data_load/DataLoad
#clsldr=com.tideworks.data_load.DataLoad.CustomClassLoader
clsldr=com.tideworks.data_load.CustomClassLoader

# avro-classes${sep}
java -ea -cp "avro-classes${sep}${CLASSPATH}" -Djava.system.class.loader=${clsldr} ${cls} "$@"
