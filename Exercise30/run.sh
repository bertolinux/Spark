# Remove folders of the previous run
#hdfs dfs -rm -r ex30_data
#hdfs dfs -rm -r ex30_out

# Put input data collection into hdfs
#hdfs dfs -put ex30_data

# Run application
#spark-submit  --class it.polito.bigdata.spark.exercise30.SparkDriver --deploy-mode cluster --master yarn target/Exercise30-1.0.0.jar "ex30_data/log.txt" ex30_out/

# Local
# Ex30 + Ex31
#spark-submit  --class it.polito.bigdata.spark.exercise30bis.SparkDriver --deploy-mode client --master local target/Exercise30-1.0.0.jar "ex30_data/log.txt" ex30_out/

# Ex32
rm -r ex30_out 
spark-submit  --class it.polito.bigdata.spark.exercise30bis.SparkDriver --deploy-mode client --master local target/Exercise30-1.0.0.jar "ex30_data/sensors.txt" ex30_out/
clear
cat ex30_out/part*
