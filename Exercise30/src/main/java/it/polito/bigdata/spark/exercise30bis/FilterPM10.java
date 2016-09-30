package it.polito.bigdata.spark.exercise30bis;

import org.apache.spark.api.java.function.Function;

// Define a class implementing the Function<String, Boolean> interface
public class FilterPM10 implements Function<String, Boolean> {
	
	Double maxPM10;
	public FilterPM10(Double max) {
		maxPM10 = max;
	}
	
	// Implement the call method
	// The call method receives one element (one string) 
	// and returns true if the element contains the word google.
	// Otherwise, it returns false.			
	public Boolean call(String line) { 
		return new Double(line.split(",")[2]).equals(maxPM10);
	}
}
