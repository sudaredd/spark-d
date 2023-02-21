package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import static org.example.Utils.getSparkContext;

@Slf4j
public class FIlterUnion {

    static int count = 0;
  public static void main(String[] args) {
      JavaSparkContext sc = getSparkContext();
      JavaRDD<String> input = sc.textFile("log.txt");

      JavaRDD<String> errorRdd = input.filter(s -> hasMsg(s, "Exception"));
      JavaRDD<String> warnRdd = input.filter(s -> hasMsg(s, "WARNING"));

      JavaRDD<String> badLinesRdd = warnRdd.union(errorRdd);
      badLinesRdd.persist(StorageLevel.MEMORY_ONLY());
      log.info("Number of bad lines {}", badLinesRdd.count());
      badLinesRdd.take(2).forEach(msg-> log.info("top bad lines {}", msg));

      badLinesRdd.unpersist();
      log.info("number of scans {}", count);
    //  badLinesRdd.foreach(msg-> log.info("message with union {}", msg));
  }

    private static boolean hasMsg(String s, String msg) {
      log.info("check hasMsg for a String {} and message {}", s, msg);
        count++;
        return s.contains(msg);
    }
}
