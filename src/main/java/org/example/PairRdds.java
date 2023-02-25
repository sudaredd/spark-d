package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.trim;
import static org.example.Utils.getSparkContext;

@Slf4j
public class PairRdds {

  public static void main(String[] args) {
    //
      JavaSparkContext sc = getSparkContext();

      JavaRDD<String> storeAddress = sc.parallelize(Arrays.asList("Ritual, 1026 Valencia St", "Starbucks, New york",
          "Philz, 748 Van Ness Ave", "Philz, 3101 24th St"));
      JavaRDD<String> storeRating = sc.parallelize(Arrays.asList("Ritual,4.9", "Philz, 4.8"));
      JavaPairRDD<String, String> storeAddressPair = storeAddress.mapToPair(str -> new Tuple2<>(str.split(",")[0], trim(str.split(",")[1])));
      JavaPairRDD<String, String> storeRatingPair = storeRating.mapToPair(str -> new Tuple2<>(str.split(",")[0], trim(str.split(",")[1])));

      JavaPairRDD<String, Tuple2<String, String>> joinPair = storeAddressPair.join(storeRatingPair);
      joinPair.foreach(val-> log.info("join key {} and values {}", val._1, val._2));
      JavaPairRDD<String, Tuple2<Optional<String>, Optional<String>>> fullJoinPair = storeAddressPair.fullOuterJoin(storeRatingPair);
      fullJoinPair.foreach(val-> log.info("full outer join key {} and values {}", val._1, val._2));
      log.info("partitioner  {}", fullJoinPair.partitioner().get().numPartitions());

  }
}
