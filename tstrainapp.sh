#!/usr/bin/env bash

#ppath="$1"
#if [ ! -n "$ppath" ] ; then
#        echo "ERROR: missing input path."
#        echo "USAGE: $0 input_path"
#        exit 1
#fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# export MASTER="yarn-client"

/usr/local/Cellar/apache-spark/2.0.2/bin/spark-submit \
    --class "com.maprps.simpletsstream.Regression" ${DIR}/target/scala-2.11/sparktsstream-assembly-0.1.0.jar --train "/Users/dmeng/Workspace/TimeSeries_QSS/ethylene_methane.csv" --resultPath "/Users/dmeng/lr2.model"

