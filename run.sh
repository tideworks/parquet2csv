#!/bin/bash

if [[ "$OSTYPE" == 'msys' ]]; then
   sep=';'
else
   sep=':'
fi

dir=./target
clspath=`find ${dir}/ -maxdepth 1 -type f -name "*.jar"|tr '\n' $sep`
if [ -z ${CLASSPATH+x} ]; then
  CLASSPATH=${clspath}
else
  CLASSPATH=${clspath}$sep${CLASSPATH}
fi

app_jar_file=
for path in ${CLASSPATH//${sep}/ }; do
#   echo "${path}"
    [[ "${path}" =~ ^.*\/parquet2csv-.*\.jar$ ]] && CLASSPATH=${path}${sep}${CLASSPATH} && app_jar_file=${path} && break
done

export HOME=${PWD}
export HADOOP_HOME=${PWD}

exec -a prq2csv java -ea -Dapp.name=prq2csv -jar ${app_jar_file} "$@"
