
ppath="$1"
if [ ! -n "$ppath" ] ; then
        echo "ERROR: missing input path."
        echo "USAGE: $0 input_path"
        exit 1
fi
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export MASTER="yarn-client"

/opt/mapr/spark/spark-1.5.2/bin/spark-submit --num-executors 3 --driver-memory 2g --executor-memory 2g --executor-cores 2 \
    --packages com.databricks:spark-csv_2.10:1.3.0 \
    --class "TSRegression" ${DIR}/target/scala-2.10/sample-ts-application_2.10-1.0.jar /user/mapr/rossmann/train.csv /user/mapr/rossmann/test.csv
