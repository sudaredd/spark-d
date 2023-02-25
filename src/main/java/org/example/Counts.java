package org.example;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.example.Utils.getSparkContext;

/** Hello world! */
@Slf4j
public class Counts {
  public static void main(String[] args) {

    JavaSparkContext sc = getSparkContext();
    JavaRDD<String> input = sc.textFile("data.txt");

    JavaRDD<String> words =
        input.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
    log.info("words {}", words.collect());

    log.info("filtering words {}", words.filter(val -> val.length() >=3).collect());



    // Transform into pairs and count.
    JavaPairRDD<String, Integer> wordsPairs = words
        .mapToPair((PairFunction<String, String, Integer>) x -> new Tuple2(x, 1));

    log.info("filter on pairs {}", wordsPairs.filter(tup-> tup._1.length() >=3 ).collect());

    log.info("wordsPairs {}", wordsPairs.collect());

    JavaPairRDD<String, Integer> counts =
        wordsPairs
            .reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

    log.info("counts {}", counts.collect());


    JavaRDD<Trade> trades = sc.parallelize(Arrays.asList(
        Trade.builder().tradeId("t1").symbol("MSFT").price(270).qty(25).buySell('B').build(),
        Trade.builder().tradeId("t2").symbol("APPL").price(165).qty(20).buySell('S').build(),
        Trade.builder().tradeId("t1").symbol("MSFT").price(276).qty(23).buySell('B').build()
    ));

    JavaPairRDD<String, Iterable<Trade>> groupByRDD = trades.groupBy(Trade::getTradeId);
    //log.info("group By => {}", groupByRDD.collect());
    log.info("trades for t2 {}", groupByRDD.lookup("t2"));

    sc.close();
 //   counts.saveAsTextFile("data_counts");

  }



  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  @ToString
  @Builder
  static class Trade implements Serializable {
    String tradeId;
    String symbol;
    int qty;
    double price;
    char buySell;
  }
}
