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
	
public class SparkDriver_help {
	public static String Test = new String();
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {

		String inputPath1=args[0];
		String inputPath2=args[1];
//		String inputPath3=args[2];
		String outputPath=args[2];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Exercise #30");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Ex30+Ex31: Filter google from log file and split distinct IP address (first column)
		// sc.textFile(inputPath).filter(new FilterGoogle()).map(new ExtractIP()).distinct().saveAsTextFile(outputPath);
				
//		FILTER Example
//		sc.textFile(inputPath)
//			.filter(
//				new Function<String, Boolean>() {
//					// Implement the call method
//					// The call method receives one element (one string) 
//					// and returns true if the element contains the word google.
//					// Otherwise, it returns false.			
//					public Boolean call(String logLine) { 
//						return logLine.toLowerCase().contains("s"); 
//					}
//				}
//			);
//		DISTINCT Example
//			.distinct()
//			.saveAsTextFile(outputPath);	
//		MAP Example
//		sc.textFile(inputPath)
//			.map(new ExtractIP());
//		FLATMAP Example
//		sc.parallelize(
//			Arrays.asList(
//				sc.textFile(inputPath)
//					.distinct()
//					.flatMap(new ExtractItems())
//					.count()
//			)
//		).saveAsTextFile(outputPath);
		
//		CARTESIAN Example
//		JavaRDD<String> rdd1 = sc.textFile(inputPath)
//				.map(new ExtractIP());
//		
//		JavaRDD<String> rdd2 = sc.textFile(inputPath);
//		
//		rdd1
//			.cartesian(rdd2)
//			.saveAsTextFile(outputPath);			
		
		// Ex32: Log sensors => top PM10 value (3 field ',' separated)
//		sc.parallelize(
//			sc.textFile(inputPath).map(
//				new Function<String,Double>() { 
//					public Double call(String a) {
//						Test = a;
//						return new Double(a.split(",")[2]);
//					}
//				}
//			).top(1)
//		).saveAsTextFile(outputPath);
		
		// Ex33: Log sensors => top PM10 value (3 field ',' separated), show top 3 to System output
//		System.out.println("Values");
		// Solution with for (Item:List)
//		for (Double item : sc.textFile(inputPath).map(
//				new Function<String,Double>() { 
//					public Double call(String a) {
//						Test = a;
//						return new Double(a.split(",")[2]);
//					}
//				}
//			).top(3)) 
		// Solution with for (int i=0...
//		List<Double> list = sc.textFile(inputPath).map(
//			new Function<String,Double>() { 
//				public Double call(String a) {
//					Test = a;
//					return new Double(a.split(",")[2]);
//				}
//			}
//		).top(3);
//		for (int i = 0; i < list.size(); i++)
//			System.out.println(list.get(i));
		
//		REDUCE Example
//		Double sum = 
//			sc.textFile(inputPath)
//				.map(new ExtractPM10())
//				.reduce(
//					new Function2<Double,Double,Double>() {
//						public Double call(Double a, Double b) {
//							return new Double(a.doubleValue()+b.doubleValue());
//						}
//					}
//				);
//		List<Double> temp = new ArrayList<Double>();
//		temp.add(sum);		
//		sc.parallelize(temp).saveAsTextFile(outputPath);

///////////////////////////////////////////////////////////////////////////////////////		
//		JAVARDDPAIR
///////////////////////////////////////////////////////////////////////////////////////
//		FILTER PAIR EXAMPLE
//		JavaRDD<String> 			rdd1	= sc.textFile(inputPath);
//		JavaPairRDD<String,Double>	rdd2	= rdd1.mapToPair(
//			new PairFunction<String,String,Double>() {
//				public Tuple2<String,Double> call(String a) {
//					String[] items = a.split(","); 
//					return new Tuple2<String,Double>(items[0],new Double(items[2]));
//				}
//			}
//		);
//		JavaPairRDD<String,Double>	rdd3	= rdd2.filter(	
//			new Function<Tuple2<String, Double>, Boolean>() {
//	
//		        public Boolean call(Tuple2<String, Double> InitiaDateCount) {
//		                if (InitiaDateCount._2()==3)
//		                        return true;
//		                else
//		                        return false;
//		        }
//			}
//	    );		
		
//		REDUCEBYKEY Example
//		JavaRDD<String> 			rdd1	= sc.textFile(inputPath);
//		JavaPairRDD<String,Double>	rdd2	= rdd1.mapToPair(
//			new PairFunction<String,String,Double>() {
//				public Tuple2<String,Double> call(String a) {
//					String[] items = a.split(","); 
//					return new Tuple2<String,Double>(items[0],new Double(items[2]));
//				}
//			}
//		);
//		JavaPairRDD	<String,Double> rdd3	= rdd2.reduceByKey(
//			new Function2<Double,Double,Double>() {
//				public Double call(Double a, Double b) {
//					if (a.doubleValue() > b.doubleValue())
//						return a;
//					return b;
//				}
//			}
//		);
//		rdd3.saveAsTextFile(outputPath);

//		COMBINEBYKEY Example
//		final Double threshold	= new Double(50);
//		JavaRDD<String> 			rdd1	= sc.textFile(inputPath).filter(
//			new Function<String,Boolean>() {
//				public Boolean call(String in) {
//					return (new Double(in.split(",")[2]).doubleValue() > threshold);
//				}
//			});
//		JavaPairRDD<String,Double>	rdd2	= rdd1.mapToPair(
//			new PairFunction<String,String,Double>() {
//				public Tuple2<String,Double> call(String a) {
//					String[] items = a.split(","); 
//					return new Tuple2<String,Double>(items[0],new Double(items[2]));
//				}
//			}
//		);
//		JavaPairRDD<String,Integer> rdd3 = rdd2.combineByKey(
//				new Function<Double,Integer>() {
//					public Integer call(Double a) {
//						return new Integer(1);
//					}
//				},
//				new Function2<Integer,Double,Integer>() {
//					public Integer call(Integer a, Double b) {
//						return a+1;
//					}
//				},
//				new Function2<Integer,Integer,Integer>() {
//					public Integer call(Integer a, Integer b) {
//						return a+b;
//					}
//				}
//		);
//		//INVERTED INDEX
//		JavaPairRDD<Integer,String>	rdd4	= rdd3.mapToPair(
//				new PairFunction<Tuple2<String, Integer>, Integer, String>() {
//					public Tuple2<Integer, String> call(Tuple2<String, Integer> inPair) {
//						return new Tuple2<Integer,String>(inPair._2(),inPair._1());
//					}
//				}
//		);
//		
//		sc.parallelize(rdd4
//			.sortByKey(false)
//			.take(1)
//		).
//		saveAsTextFile(outputPath);		
		
//		GROUPBYKEY Example
//		JavaRDD<String> 			rdd1	= sc.textFile(inputPath);
//		JavaPairRDD<String,String>	rdd2	= rdd1.mapToPair(
//			new PairFunction<String,String,String>() {
//				public Tuple2<String,String> call(String a) {
//					String[] items = a.split(","); 
//					return new Tuple2<String,String>(items[0],items[1]);
//				}
//			}
//		).distinct();
//		JavaPairRDD	<String,Iterable<String>> rdd3	= rdd2.groupByKey();
//		rdd3.saveAsTextFile(outputPath);		
		
//		COGROUP+JOIN Example
//		(Q2,([Who invented ..],[A2 John Smith]))
//		(Q1,([What is ..?],[A1 It is .., A3 I think it is ..]))
//		(Q2,(Who invented ..,A2 John Smith))
//		(Q1,(What is ..?,A1 It is ..))
//		(Q1,(What is ..?,A3 I think it is ..))
//		JavaRDD<String> 			rdd1	= sc.textFile(inputPath1);
//		JavaPairRDD<String,String>	rddQ	= rdd1.mapToPair(
//			new PairFunction<String,String,String>() {
//				public Tuple2<String,String> call(String a) {
//					String[] items = a.split(","); 
//					return new Tuple2<String,String>(items[0],items[2]);
//				}
//			}
//		);
//		rddQ.saveAsTextFile(outputPath1);		
//		JavaRDD<String> 			rdd2	= sc.textFile(inputPath2);
//		JavaPairRDD<String,String>	rddA	= rdd2.mapToPair(
//			new PairFunction<String,String,String>() {
//				public Tuple2<String,String> call(String a) {
//					String[] items = a.split(","); 
//					return new Tuple2<String,String>(items[1],items[0]+" "+items[3]);
//				}
//			}
//		);
//		rddA.saveAsTextFile(outputPath2);	
//		
//		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> rddC =
//				rddQ.cogroup(rddA);
//				
//		JavaPairRDD<String,Tuple2<String,String>> rddD =
//				rddQ.join(rddA);
//		
////		rddC.saveAsTextFile(outputPath);
//		rddD.saveAsTextFile(outputPath);		
		
//		JavaRDD<String> rdd1 = sc.textFile(inputPath).filter(
//				new Function<String,Boolean>() {
//					public Boolean call(String a) {
//						String[] items = a.split(",");
//						if (new Float(items[2])>50)
//							return true;
//						return false;
//					}
//				}
//		);
////		rdd1.saveAsTextFile(outputPath);
//		
//		//MAPTOPAIR
//		JavaPairRDD<String,Integer> rdd2 = rdd1.mapToPair(
//				new PairFunction<String,String,Integer>() {
//					public Tuple2<String,Integer> call(String a) {
//						String[] items = a.split(",");
//						return new Tuple2<String,Integer>(items[0],new Integer(1)); 
//					}
//				}
//		);
//		//REDUCEBYKEY (SUM)
//		JavaPairRDD<String,Integer> rdd3 = rdd2.reduceByKey(
//				new Function2<Integer,Integer,Integer>() {
//					public Integer call(Integer a,Integer b) {
//						return a+b;
//					}
//				}
//		);
//		//INVERTED INDEX
//		JavaPairRDD<Integer,String> rdd4 = rdd3.mapToPair(
//				new PairFunction<Tuple2<String,Integer>,Integer,String>() {
//					public Tuple2<Integer,String> call(Tuple2<String,Integer> t) {
//						return new Tuple2<Integer,String>(t._2(),t._1());
//					}
//				}
//		);
//		//SORTBYKEY(DESCENDING)
//		JavaPairRDD<Integer,String> rdd5 = rdd4.sortByKey(false);
		
		
//		rdd5.saveAsTextFile(outputPath);
		
//		JOIN, COGROUP, MAPTOPAIR
//		JavaPairRDD<String,String> film_visti = sc.textFile(inputPath1).mapToPair(
//				new PairFunction<String,String,String>() {
//					public Tuple2<String,String> call(String row) {
//						String[] items = row.split(",");
//						return new Tuple2<String,String>(items[0],items[1]);
//					}
//				}
//		);
//		JavaPairRDD<String,String> films = sc.textFile(inputPath2).mapToPair(
//				new PairFunction<String,String,String>() {
//					public Tuple2<String,String> call(String row) {
//						String[] items = row.split(",");
//						return new Tuple2<String,String>(items[0],items[1]);
//					}
//				}
//		);
//		JavaPairRDD<String,String> generi_preferiti = sc.textFile(inputPath3).mapToPair(
//				new PairFunction<String,String,String>() {
//					public Tuple2<String,String> call(String row) {
//						String[] items = row.split(",");
//						return new Tuple2<String,String>(items[0],items[1]);
//					}
//				}
//		);
//		JavaPairRDD<String,Tuple2<String,String>> film_con_generi_visti = film_visti.join(films);
//		JavaPairRDD<String,String> generi_visti = film_con_generi_visti.mapToPair(
//				new PairFunction<Tuple2<String,Tuple2<String,String>>,String,String>() {
//					public Tuple2<String,String> call(Tuple2<String,Tuple2<String,String>> t) {
//						return new Tuple2<String,String>(t._2()._1(),t._2()._2());
//					}
//				}
//		);	
//		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> generi_visti_e_preferiti = generi_visti.cogroup(generi_preferiti);
//		JavaPairRDD<String,Tuple2<Iterable<String>,Iterable<String>>> filtra_generi_visti_e_preferiti_per_utente = generi_visti_e_preferiti.filter(
//				new Function<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>,Boolean>(){
//					public Boolean call(Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>> t) {
//						for (String value: t._2()._1()) {
//							
//						}
//						return false;
//					}
//				}
//		);
//		JavaPairRDD<String,String> conta_generi_visti_e_preferiti_per_utente = generi_visti_e_preferiti.mapToPair(
//				new PairFunction<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>,String,String>(){
//					public Tuple2<String,String> call(Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>> t) {
//						String SumString = new String();
//						int num = 0;
//						for (String value: t._2()._1()) {
//							num++;
//						}
//						SumString += new Integer(num).toString();
//						num = 0;
//						for (String value: t._2()._2()) {
//							num++;
//						}
//						SumString += " " + new Integer(num).toString();
//						
//						return new Tuple2<String,String>(t._1(),SumString);
//					}
//				}
//		);
//		conta_generi_visti_e_preferiti_per_utente.saveAsTextFile(outputPath);
		
//		JavaPairRDD<String,String> film1 = sc.textFile(inputPath1).mapToPair(new readFilms());
//		JavaPairRDD<String,String> film2 = sc.textFile(inputPath2).mapToPair(new readFilms());
//		
//		JavaPairRDD<String,String> film3 = film1.intersection(film2);
//		film1.saveAsTextFile(outputPath);

		
		
		
		// Close the Spark context
		sc.close();
	}
}
