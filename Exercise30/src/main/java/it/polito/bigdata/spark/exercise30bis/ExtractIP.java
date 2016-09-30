package it.polito.bigdata.spark.exercise30bis;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class ExtractIP implements Function<String, String> {
	public String call(String a) {
		return a.split(",")[0].toString();
	}
}
