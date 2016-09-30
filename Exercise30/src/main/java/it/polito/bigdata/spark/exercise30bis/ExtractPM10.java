package it.polito.bigdata.spark.exercise30bis;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class ExtractPM10 implements Function<String, Double> {
	public Double call(String a) {
		return new Double(a.split(",")[2].toString());
	}
}
