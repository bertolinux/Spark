rm -rf out
mvn package
spark-submit  --jars target/geospark-0.3.jar --class it.polito.bigdata.spark.AnalisiGeoSpark.SparkDriver --deploy-mode client --master local target/AnalisiGeoSpark-1.0.0.jar "in" "out"
find |grep part|grep -v "\.part"|while read FILE; do echo "$FILE"; cat "$FILE"; echo "";done
