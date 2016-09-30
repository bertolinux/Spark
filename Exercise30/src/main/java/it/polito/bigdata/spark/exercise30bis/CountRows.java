package it.polito.bigdata.spark.exercise30bis;

import org.apache.spark.api.java.function.Function2;

public class CountRows implements Function2<Double,Double, Integer> {
	public Integer call(Double a, Double b) {
		return new Integer(2);
	}
}
