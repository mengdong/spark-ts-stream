#!/usr/bin/env bash

#ppath="$1"
#if [ ! -n "$ppath" ] ; then
#        echo "ERROR: missing input path."
#        echo "USAGE: $0 input_path"
#        exit 1
#fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export MASTER="yarn-client"

/opt/mapr/spark/spark-2.0.1/bin/spark-submit --master local[2] \
    --class "com.maprps.simpletsstream.Regression" ${DIR}/target/scala-2.11/spark-ts-stream-assembly-0.1.0.jar \
    --train "/user/mapr/ethylene_methane_1m.csv" --regressionPath "/user/mapr/lr.model" --classificationPath "/user/mapr/log.model"

