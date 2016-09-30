# Remove folders of the previous run
# Test
rm -r out 
spark-submit  --jars target/original-geospark-0.3.jar --class it.polito.bigdata.spark.exercise30bis.SparkDriver --deploy-mode client --master local target/Exercise30-1.0.0.jar "In1" "In2" "out"
clear
cat out/part*
