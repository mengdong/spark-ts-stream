#!/usr/bin/env bash

#ppath="$1"
#if [ ! -n "$ppath" ] ; then
#        echo "ERROR: missing input path."
#        echo "USAGE: $0 input_path"
#        exit 1
#fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# export MASTER="yarn-client"

/opt/mapr/spark/spark-2.0.1/bin/spark-submit --local[2] --executor-memory 1G \
    --class "com.maprps.simpletsstream.RunTS" ${DIR}/target/scala-2.11/spark-ts-stream-assembly-0.1.0.jar \
    --rModelLoc "/user/mapr/lr.model" --cModelLoc "tobeContinue"

