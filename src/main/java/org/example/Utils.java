package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Utils {

  public static JavaSparkContext getSparkContext() {
    return getSparkContext("My App");
  }

  public static JavaSparkContext getSparkContext(String app) {
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(app);
    JavaSparkContext sc = new JavaSparkContext(conf);
    return sc;
  }
}
