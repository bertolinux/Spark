package it.polito.bigdata.spark.exercise30bis;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class readFilms implements PairFunction<String,String,String> {
	public Tuple2<String,String> call(String a) {
		String[] items = a.split(",");
		return new Tuple2<String,String>(items[0],items[1]);
	}
}
