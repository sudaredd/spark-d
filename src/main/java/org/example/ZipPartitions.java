package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import static org.example.Utils.getSparkContext;

@Slf4j
public class ZipPartitions {

  public static void main(String[] args) {

    JavaSparkContext sc = getSparkContext();

    JavaRDD<String> firstL = sc.parallelize(IntStream.rangeClosed(1, 1000).mapToObj(String::valueOf).collect(Collectors.toList()), 10);
    JavaRDD<String> secondl = sc.parallelize(IntStream.rangeClosed(1001, 2000).mapToObj(String::valueOf).collect(Collectors.toList()), 10);

    JavaRDD<String> javaZipRDD = firstL.zipPartitions(secondl, (ite1, ite2) -> {
      List<String> list = new ArrayList();
      String uniqueId = UUID.randomUUID().toString();
      log.info("uniqueId on the partition {}", uniqueId);
      while (ite1.hasNext() && ite2.hasNext()) {
        list.add(uniqueId + " "+ ite1.next() + " " + ite2.next());
      }
      return list.iterator();
    });

    javaZipRDD.foreach(val -> log.info("value {}", val));
    Utils.sleep(100000);
  }
}
