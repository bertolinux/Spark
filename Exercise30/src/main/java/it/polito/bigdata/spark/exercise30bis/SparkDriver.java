package it.polito.bigdata.spark.exercise30bis;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.spark.SparkConf;
	
public class SparkDriver {
	public static String Test = new String();
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {

		String inputPath1=args[0];
		String inputPath2=args[1];
		String outputPath=args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		
		// Close the Spark context
		sc.close();
	}
}
