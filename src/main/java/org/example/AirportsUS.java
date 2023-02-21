package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

import static org.example.Utils.getSparkContext;

/** Hello world! */
@Slf4j
public class AirportsUS {
  public static void main(String[] args) {

    JavaSparkContext sc = getSparkContext();

    JavaRDD<String> input = sc.textFile("in/ny.txt");


    JavaRDD<String> words =
        input.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
    log.info("words {}", words.collect());

    Map<String, Long> wordCounts = words.countByValue();

    wordCounts.forEach((k, v)-> log.info(k + ":" + v));


    sc.close();
 //   counts.saveAsTextFile("data_counts");

  }



}
