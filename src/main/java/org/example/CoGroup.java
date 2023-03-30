package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.example.Utils.getSparkContext;

@Slf4j
public class CoGroup {

  public static void main(String[] args) {

    JavaSparkContext sc = getSparkContext();

    JavaPairRDD<String, String> categories = sc.parallelizePairs(Arrays.asList(
        new Tuple2<>("Apples", "Fruit"),
        new Tuple2<>("Oranges", "Fruit"),
        new Tuple2<>("Oranges", "Citrus")
    ));
    JavaPairRDD<String, Integer> prices = sc.parallelizePairs(Arrays.asList(
        new Tuple2<>("Oranges", 2),
        new Tuple2<>("Apples", 3)
    ));
    JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogrouped =
        categories.cogroup(prices);

    cogrouped.foreach((tup) -> log.info("value is {}", tup._2));
    Utils.sleep(100000);
  }
}
