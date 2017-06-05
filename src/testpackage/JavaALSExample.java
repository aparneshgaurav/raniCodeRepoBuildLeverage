package org.sparkexample;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;





import org.apache.spark.sql.types.StructType;

import java.io.IOException;
// $example on$
import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
// $example off$ ../bin/spark-submit --class org.sparkexample.JavaALSExample --master local[2] ../../test-0.0.1-SNAPSHOT.jar

public class JavaALSExample {

  // $example on$
  public static class Rating implements Serializable {
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public Rating() {}

    public Rating(int userId, int movieId, float rating, long timestamp) {
      this.userId = userId;
      this.movieId = movieId;
      this.rating = rating;
      this.timestamp = timestamp;
    }

    public int getUserId() {
      return userId;
    }

    public int getMovieId() {
      return movieId;
    }

    public float getRating() {
      return rating;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public static Rating parseRating(String str) {
      String[] fields = str.split(",");
      if (fields.length != 4) {
        throw new IllegalArgumentException("Each line must contain 4 fields");
      }
      int userId = Integer.parseInt(fields[0]);
      int movieId = Integer.parseInt(fields[1]);
      float rating = Float.parseFloat(fields[2]);
      long timestamp = Long.parseLong(fields[3]);
      return new Rating(userId, movieId, rating, timestamp);
    }
  }
  // $example off$

  public static void main(String[] args) throws IOException {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaALSExample")
      .getOrCreate();

    // $example on$
    JavaRDD<Rating> ratingsRDD = spark
      .read().textFile("/home/cloudera/dev/ml-latest-small/ratings1.csv").javaRDD()
      .map(new Function<String, Rating>() {
    	    public Rating call(String str) {
    	        return Rating.parseRating(str);
    	      }
    	    });
    Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
    Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
    Dataset<Row> training = splits[0];
    Dataset<Row> test = splits[1];

    // Build the recommendation model using ALS on the training data
    ALS als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating");
    
    Pipeline pipeLine = new Pipeline().setStages(new PipelineStage[]{als});
//    Pipeline pipeLine = new pipeli
    
    
//    ALSModel model = als.fit(training);
    PipelineModel pipelineModel = pipeLine.fit(training);
    
    
//    PipelineModel ppModel = als.fit(training);
//    model.
    pipelineModel.save("/home/cloudera/dev/jars/als.ser");

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
//    model.setColdStartStrategy("drop");
    Dataset<Row> predictions = pipelineModel.transform(test).na().drop();
    
//    predictions.
    
    System.out.println("######################"+predictions.count());
    System.out.println("json : "+predictions.toJSON().toString());
    
    for (Row row : predictions.select("userId", "movieId", "rating").collectAsList()) {
    		System.out.println("hello");
    	  System.out.println("(" + row.get(0) + ", " + row.get(1) + ") --> prediction=" + row.get(2));
    	}

    RegressionEvaluator evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction");
    Double rmse = evaluator.evaluate(predictions);
    System.out.println("Root-mean-square error = " + rmse);
  
    // $example off$
    spark.stop();
  }
  
  
  
  
  
  
}