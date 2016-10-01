package it.polito.bigdata.spark.AnalisiGeoSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
	
public class SparkDriver {
	// Counter for incrementing lines
	public static int counter = 0;
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		//Input PATH for importing CSV
		String directory_in = args[0];
		
		// Output PATH for exporting data
		String directory_out = args[1];

		// CSV Files to read from
		String pointLocation = directory_in + "/" + "point.csv";
		String rectangleLocation = directory_in + "/" + "rectangle.csv";
		String polygonLocation = directory_in + "/" + "polygon.csv";
		
		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Analisi GeoSpark");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// POINT
		// Read PointRDD structure
		PointRDD point_rdd = new PointRDD(sc,pointLocation,0,"csv");
		// Save as GeoJSON
		point_rdd.saveAsGeoJSON(directory_out + "/geojson");
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
		}).saveAsTextFile(directory_out + "/point");
		// Reset line counter
		counter = 0;
		
		// RECTANGLE and ENVELOPE 
		// Note: Rnvelope class is used because Rectangle is already used on java.awt package 
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
		}).saveAsTextFile(directory_out + "/envelope");
		// Reset line counter
		counter = 0;
				
		// Polygon
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
		}).saveAsTextFile(directory_out + "/polygon");
		// Reset line counter
		counter = 0;
		
		// Close the Spark context
		sc.close();
		 
	}
}
