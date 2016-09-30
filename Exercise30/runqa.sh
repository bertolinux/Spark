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
rm -r ex42_out 
rm -r ex42_out1 
rm -r ex42_out2 
spark-submit  --class it.polito.bigdata.spark.exercise30bis.SparkDriver --deploy-mode client --master local target/Exercise30-1.0.0.jar "ex42_data_questions/" "ex42_data_answers/" ex42_out1/ ex42_out2/ ex42_out/
clear
cat ex42_out1/part*
cat ex42_out2/part*
echo
cat ex42_out/part*
