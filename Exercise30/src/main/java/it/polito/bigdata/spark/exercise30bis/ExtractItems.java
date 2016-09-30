package it.polito.bigdata.spark.exercise30bis;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;

@SuppressWarnings("serial")
public class ExtractItems implements FlatMapFunction<String, String> {
	public Iterable<String> call(String a) {
		return Arrays.asList(a.split(","));
	}
}
