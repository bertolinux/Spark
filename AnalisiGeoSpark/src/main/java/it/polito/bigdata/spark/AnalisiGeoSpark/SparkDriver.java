package it.polito.bigdata.spark.AnalisiGeoSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
	
public class SparkDriver {
	// Counter for incrementing lines
	public static int counter = 0;
	public static Envelope firstEnvelope = null;
	
	@SuppressWarnings("serial")
	private static void managePoint(JavaSparkContext sc, String inLocation, String outLocation) {
		// Read PointRDD structure
		String pointLocation = inLocation + "/" + "point.csv";
		
		PointRDD point_rdd = new PointRDD(sc,pointLocation,0,"csv");
		// Save as GeoJSON
		point_rdd.saveAsGeoJSON(outLocation + "/geojson");
		// Create Standard JavaRDD<Point>
		JavaRDD <Point> point_javardd = point_rdd.getRawPointRDD();
	
		// Reset line counter
		counter = 0;
		// Read each line
		point_javardd.map(new Function<Point,String>() {
			public String call(Point point) {
				String output = "";
				// Extract all in one String
				output += "Point (" + counter + "): " + point.toString();
				// Extract X and Y
				output += " X: " + point.getX();
				output += " Y: " + point.getY();
				counter++;
				return output;
			}
		// Save all in point directory
		}).saveAsTextFile(outLocation + "/point");
		// Reset line counter
		counter = 0;
	}
	
	private static void manageRectangle(JavaSparkContext sc, String inLocation, String outLocation) {
		// Path to CSV file
		String rectangleLocation = inLocation + "/" + "rectangle.csv";
		
		// Note: Envelope class is used because Rectangle is already used on java.awt package 
		// Read RectangleRDD structure
		RectangleRDD rectangle_rdd = new RectangleRDD(sc,rectangleLocation,0,"csv");
		// Create Standard JavaRDD<Envelop>
		JavaRDD <Envelope> rectangle_javardd = rectangle_rdd.getRawRectangleRDD();
		// Reset line counter
		counter = 0;
		// Read each line
		rectangle_javardd.map(new Function<Envelope,String>() {
			public String call(Envelope envelope) {
				String output = "";
				// Extract all in one String
				output += "Envelope (" + counter + "): " + envelope.toString();
				// Extract other attributes
				output += " MaxX: " + envelope.getMaxX();
				output += " MaxY: " + envelope.getMaxY();
				output += " MinX: " + envelope.getMinX();
				output += " MinY: " + envelope.getMinY();
				output += "\n";
				output += "Width: " + envelope.getWidth();
				output += " Height: " + envelope.getHeight();
				output += " Area: " + envelope.getArea();
				output += "\n";
				counter++;
				return output;
			}
		// Save all in envelope directory
		}).saveAsTextFile(outLocation + "/envelope");
		// Reset line counter
		counter = 0;		
	}
	
	private static void managePolygon(JavaSparkContext sc, String inLocation, String outLocation) {
		// CSV Files to read from
		String polygonLocation = inLocation + "/" + "polygon.csv";
				
		// Read PolygonRDD structure
		PolygonRDD polygon_rdd = new PolygonRDD(sc,polygonLocation,0,"csv");
		// Create Standard JavaRDD<Polygon>
		JavaRDD <Polygon> polygon_javardd = polygon_rdd.getRawPolygonRDD();
		// Reset line counter
		counter = 0;
		// Read each line
		polygon_javardd.map(new Function<Polygon,String>() {
			public String call(Polygon polygon) {
				String output = "";
				// Extract all in one String
				output += "Polygon (" + counter + "): " + polygon.toString();
				// Extract other attributes
				output += " Dimension: " + polygon.getDimension();
				output += " Length: " + polygon.getLength();
				output += " Num Points: " + polygon.getNumPoints();
				output += " Area: " + polygon.getArea();
				output += "\n";
				counter++;
				return output;
			}
		// Save all in polygon directory
		}).saveAsTextFile(outLocation + "/polygon");
		// Reset line counter
		counter = 0;	
	}	
	
	@SuppressWarnings("serial")
	private static void envelopeOperations(JavaSparkContext sc, String inLocation, String outLocation) {
		// Path to CSV file
		String rectangleLocation = inLocation + "/" + "rectangle.csv";
		
		// Note: Envelope class is used because Rectangle is already used on java.awt package 
		// Read RectangleRDD structure
		RectangleRDD rectangle_rdd = new RectangleRDD(sc,rectangleLocation,0,"csv");
		
		
		// Create Standard JavaRDD<Envelop>
		JavaRDD <Envelope> rectangle_javardd = rectangle_rdd.getRawRectangleRDD();
		
		// Reset line counter
		counter = 0;
		firstEnvelope = null;
		// Read each line
		rectangle_javardd.map(new Function<Envelope,String>() {
			public String call(Envelope envelope) {
				String output = "";
				
				if (firstEnvelope != null)
					output += "firstEnvelope contains envelope (before expanding): " + firstEnvelope.contains(envelope);
					output += "\n";
				
				if (counter == 0)
					firstEnvelope = envelope;
				else
					firstEnvelope.expandToInclude(envelope);
				
				// Extract all in one String
				output += "Envelope (" + counter + "): " + envelope.toString();
				// Extract other attributes
				output += " MaxX: " + envelope.getMaxX();
				output += " MaxY: " + envelope.getMaxY();
				output += " MinX: " + envelope.getMinX();
				output += " MinY: " + envelope.getMinY();
				output += "\n";
				output += "Width: " + envelope.getWidth();
				output += " Height: " + envelope.getHeight();
				output += " Area: " + envelope.getArea();
				output += "\n";

				// Extract all in one String
				output += "firstEnvelope (" + counter + "): " + firstEnvelope.toString();
				// Extract other attributes
				output += " MaxX: " + firstEnvelope.getMaxX();
				output += " MaxY: " + firstEnvelope.getMaxY();
				output += " MinX: " + firstEnvelope.getMinX();
				output += " MinY: " + firstEnvelope.getMinY();
				output += "\n";
				output += "Width: " + firstEnvelope.getWidth();
				output += " Height: " + firstEnvelope.getHeight();
				output += " Area: " + firstEnvelope.getArea();
				
				// Retrieve Central position of firstEnvelope (whichi in increasing on each step)
				output += " Center: " + firstEnvelope.centre().toString();
				output += " firstEnvelope contains envelope (after expanding): " + firstEnvelope.contains(envelope);
				
				output += "\n";
				output += "\n";
				counter++;
				return output;
			}
		// Save all in envelope directory
		}).saveAsTextFile(outLocation + "/expandToInclude");
		// Reset line counter
		counter = 0;
	}
	
	@SuppressWarnings("serial")
	private static void envelopeIncludePoint(JavaSparkContext sc, String inLocation, String outLocation) {
		// Path to CSV file for rectangles
		String rectangleLocation = inLocation + "/" + "rectangle.csv";
		// Read rectangles
		RectangleRDD rectangle_rdd = new RectangleRDD(sc,rectangleLocation,0,"csv");
		// Create Standard JavaRDD<Envelop>
		JavaRDD <Envelope> rectangle_javardd = rectangle_rdd.getRawRectangleRDD();
		
		
		// Path to CSV file for points
		String pointLocation = inLocation + "/" + "point.csv";
		// Read points
		PointRDD point_rdd = new PointRDD(sc,pointLocation,0,"csv");
		// Create Standard JavaRDD<Point>
		JavaRDD <Point> point_javardd = point_rdd.getRawPointRDD();

		// Create List from RDD (just for these tests)
		List<Envelope> envelope_list = rectangle_javardd.collect();
		List<Point> point_list = point_javardd.collect();
		
		Envelope envelope = envelope_list.get(0);
		Point point = point_list.get(2);
		
		List <String> file = new ArrayList<String>();
		
		file.add("Envelope: " + envelope.toString());
		// Extract other attributes
		file.add(" MaxX: " + envelope.getMaxX());
		file.add(" MaxY: " + envelope.getMaxY());
		file.add(" MinX: " + envelope.getMinX());
		file.add(" MinY: " + envelope.getMinY());
		file.add(" Width: " + envelope.getWidth());
		file.add(" Height: " + envelope.getHeight());
		file.add(" Area: " + envelope.getArea());
		file.add("");
		
		// Extract all in one String
		file.add("Point: " + point.toString());
		// Extract X and Y
		file.add(" X: " + point.getX());
		file.add(" Y: " + point.getY());
		file.add("");
		
		// Expand Envelope to include the point
		envelope.expandToInclude(point.getX(), point.getY());
		file.add(" New Envelope including point: " + envelope.toString());
		// Extract other attributes
		file.add(" MaxX: " + envelope.getMaxX());
		file.add(" MaxY: " + envelope.getMaxY());
		file.add(" MinX: " + envelope.getMinX());
		file.add(" MinY: " + envelope.getMinY());
		file.add(" Width: " + envelope.getWidth());
		file.add(" Height: " + envelope.getHeight());
		file.add(" Area: " + envelope.getArea());
		
		sc.parallelize(file).saveAsTextFile(outLocation + "/envelopeIncludePoint");		
	}	
	
	@SuppressWarnings("serial")
	private static void envelopeIntersection(JavaSparkContext sc, String inLocation, String outLocation) {
		// Path to CSV file for rectangles
		String rectangleLocation = inLocation + "/" + "rectangle.csv";
		// Read rectangles
		RectangleRDD rectangle_rdd = new RectangleRDD(sc,rectangleLocation,0,"csv");
		// Create Standard JavaRDD<Envelop>
		JavaRDD <Envelope> rectangle_javardd = rectangle_rdd.getRawRectangleRDD();

		// Create List from RDD (just for these tests)
		List<Envelope> envelope_list = rectangle_javardd.collect();
		
		Envelope envelope1 = envelope_list.get(0);
		Envelope envelope2 = envelope_list.get(2);
		
		List <String> file = new ArrayList<String>();
		
		file.add("Envelope1: " + envelope1.toString());
		// Extract other attributes
		file.add(" MaxX: " + envelope1.getMaxX());
		file.add(" MaxY: " + envelope1.getMaxY());
		file.add(" MinX: " + envelope1.getMinX());
		file.add(" MinY: " + envelope1.getMinY());
		file.add(" Width: " + envelope1.getWidth());
		file.add(" Height: " + envelope1.getHeight());
		file.add(" Area: " + envelope1.getArea());
		file.add("");

		file.add("Envelope2: " + envelope2.toString());
		// Extract other attributes
		file.add(" MaxX: " + envelope2.getMaxX());
		file.add(" MaxY: " + envelope2.getMaxY());
		file.add(" MinX: " + envelope2.getMinX());
		file.add(" MinY: " + envelope2.getMinY());
		file.add(" Width: " + envelope2.getWidth());
		file.add(" Height: " + envelope2.getHeight());
		file.add(" Area: " + envelope2.getArea());
		file.add("");
		
		// Envelope Intersection
		envelope2.intersection(envelope1);
		file.add(" New Envelope resulting from intersection: " + envelope2.toString());
		// Extract other attributes
		file.add(" MaxX: " + envelope2.getMaxX());
		file.add(" MaxY: " + envelope2.getMaxY());
		file.add(" MinX: " + envelope2.getMinX());
		file.add(" MinY: " + envelope2.getMinY());
		file.add(" Width: " + envelope2.getWidth());
		file.add(" Height: " + envelope2.getHeight());
		file.add(" Area: " + envelope2.getArea());
		file.add("");
		
		sc.parallelize(file).saveAsTextFile(outLocation + "/envelopeIntersection");		
	}		
	
	private static void polygonIntersection(JavaSparkContext sc, String inLocation, String outLocation) {
		// CSV Files to read from
		String polygonLocation = inLocation + "/" + "polygon.csv";
				
		// Read PolygonRDD structure
		PolygonRDD polygon_rdd = new PolygonRDD(sc,polygonLocation,0,"csv");
		// Create Standard JavaRDD<Polygon>
		JavaRDD <Polygon> polygon_javardd = polygon_rdd.getRawPolygonRDD();

		// Create List from RDD (just for these tests)
		List<Polygon> polygon_list = polygon_javardd.collect();
		
		Polygon polygon1 = polygon_list.get(0);
		Polygon polygon2 = polygon_list.get(5);
		
		List <String> file = new ArrayList<String>();
		
		file.add("polygon1: " + polygon1.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon1.getDimension());
		file.add(" Length: " + polygon1.getLength());
		file.add(" Num Points: " + polygon1.getNumPoints());
		file.add(" Area: " + polygon1.getArea());
		file.add("");

		file.add("polygon2: " + polygon2.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon2.getDimension());
		file.add(" Length: " + polygon2.getLength());
		file.add(" Num Points: " + polygon2.getNumPoints());
		file.add(" Area: " + polygon2.getArea());
		file.add("");
		
		file.add("polygon2 intesects polygon1: " + polygon2.intersects(polygon1));
		file.add("");
		
		Polygon polygon3 = (Polygon) polygon2.intersection(polygon1);
		file.add("polygon3 (interesection between polygon1 and polygon2: " + polygon3.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon3.getDimension());
		file.add(" Length: " + polygon3.getLength());
		file.add(" Num Points: " + polygon3.getNumPoints());
		file.add(" Area: " + polygon3.getArea());
		
		file.add("");		
		sc.parallelize(file).saveAsTextFile(outLocation + "/polygonIntersection");		
	}	
	
	private static void polygonDifference(JavaSparkContext sc, String inLocation, String outLocation) {
		// CSV Files to read from
		String polygonLocation = inLocation + "/" + "polygon.csv";
				
		// Read PolygonRDD structure
		PolygonRDD polygon_rdd = new PolygonRDD(sc,polygonLocation,0,"csv");
		// Create Standard JavaRDD<Polygon>
		JavaRDD <Polygon> polygon_javardd = polygon_rdd.getRawPolygonRDD();

		// Create List from RDD (just for these tests)
		List<Polygon> polygon_list = polygon_javardd.collect();
		
		Polygon polygon1 = polygon_list.get(1);
		Polygon polygon2 = polygon_list.get(0);
		
		List <String> file = new ArrayList<String>();
		
		file.add("polygon1: " + polygon1.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon1.getDimension());
		file.add(" Length: " + polygon1.getLength());
		file.add(" Num Points: " + polygon1.getNumPoints());
		file.add(" Area: " + polygon1.getArea());
		file.add("");

		file.add("polygon2: " + polygon2.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon2.getDimension());
		file.add(" Length: " + polygon2.getLength());
		file.add(" Num Points: " + polygon2.getNumPoints());
		file.add(" Area: " + polygon2.getArea());
		file.add("");
		
		file.add("polygon2 intesects polygon1: " + polygon2.intersects(polygon1));
		file.add("");
		
		polygon2.difference(polygon1);
		Polygon polygon3 = (Polygon) polygon2.difference(polygon1);
		file.add("polygon3 (difference between polygon1 and polygon2: " + polygon3.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon3.getDimension());
		file.add(" Length: " + polygon3.getLength());
		file.add(" Num Points: " + polygon3.getNumPoints());
		file.add(" Area: " + polygon3.getArea());
		
		file.add("");		
		sc.parallelize(file).saveAsTextFile(outLocation + "/polygonDifference");		
	}	
	
	private static void polygonUnion(JavaSparkContext sc, String inLocation, String outLocation) {
		// CSV Files to read from
		String polygonLocation = inLocation + "/" + "polygon.csv";
				
		// Read PolygonRDD structure
		PolygonRDD polygon_rdd = new PolygonRDD(sc,polygonLocation,0,"csv");
		// Create Standard JavaRDD<Polygon>
		JavaRDD <Polygon> polygon_javardd = polygon_rdd.getRawPolygonRDD();

		// Create List from RDD (just for these tests)
		List<Polygon> polygon_list = polygon_javardd.collect();
		
		Polygon polygon1 = polygon_list.get(6);
		Polygon polygon2 = polygon_list.get(7);
		
		List <String> file = new ArrayList<String>();
		
		file.add("polygon1: " + polygon1.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon1.getDimension());
		file.add(" Length: " + polygon1.getLength());
		file.add(" Num Points: " + polygon1.getNumPoints());
		file.add(" Area: " + polygon1.getArea());
		file.add("");

		file.add("polygon2: " + polygon2.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon2.getDimension());
		file.add(" Length: " + polygon2.getLength());
		file.add(" Num Points: " + polygon2.getNumPoints());
		file.add(" Area: " + polygon2.getArea());
		file.add("");
		
		file.add("polygon2 intesects polygon1: " + polygon2.intersects(polygon1));
		file.add("");
		
		MultiPolygon polygon3 = (MultiPolygon) polygon2.union(polygon1);
		file.add("polygon3 (union between polygon1 and polygon2: " + polygon3.toString());
		// Extract other attributes
		file.add(" Dimension: " + polygon3.getDimension());
		file.add(" Length: " + polygon3.getLength());
		file.add(" Num Points: " + polygon3.getNumPoints());
		file.add(" Area: " + polygon3.getArea());
		
		file.add("");		
		sc.parallelize(file).saveAsTextFile(outLocation + "/polygonUnion");		
	}	
	
	public static void main(String[] args) {
		//Input PATH for importing CSV
		String directory_in = args[0];
		
		// Output PATH for exporting data
		String directory_out = args[1];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Analisi GeoSpark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// POINT
		managePoint(sc, directory_in, directory_out);
		
		// RECTANGLE
		manageRectangle(sc, directory_in, directory_out);

		// POLYGON
		managePolygon(sc, directory_in, directory_out);
		
		// envelopeOperations
		envelopeOperations(sc, directory_in, directory_out);
		
		// envelopeIncludePoint
		envelopeIncludePoint(sc, directory_in, directory_out);
		
		// envelopeIntersection
		envelopeIntersection(sc, directory_in, directory_out);
		
		// polygonOperations
		polygonIntersection(sc, directory_in, directory_out);
		
		// polygonDifference
		polygonDifference(sc, directory_in, directory_out);
		
		// polygonUnion
		polygonUnion(sc, directory_in, directory_out);
		
		// Close the Spark context
		sc.close();
		 
	}
}
