package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.example.Utils.getSparkContext;

@Slf4j
public class ZipPartitions {

    public static void main(String[] args) {

        JavaSparkContext sc = getSparkContext();

        JavaRDD<String> firstL = sc.parallelize(IntStream.rangeClosed(1, 100000).mapToObj(String::valueOf).collect(Collectors.toList()), 100);
        JavaRDD<String> secondl = sc.parallelize(IntStream.rangeClosed(100001, 200000).mapToObj(String::valueOf).collect(Collectors.toList()), 100);

        JavaRDD<String> firstWIndex = firstL.mapPartitionsWithIndex((partId, iterator) -> {
            List<String> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add("preserve1 " + iterator.next());
            }
            return list.iterator();
        }, false);

        JavaRDD<String> secondWIndex = secondl.mapPartitionsWithIndex((partId, iterator) -> {
            List<String> list = new ArrayList<>();
            while (iterator.hasNext()) {
                list.add("preserve2 " + iterator.next());
            }
            return list.iterator();
        }, false);

        JavaRDD<String> javaZipRDD = firstWIndex.zipPartitions(secondWIndex, (ite1, ite2) -> {
            List<String> list = new ArrayList();
            String uniqueId = UUID.randomUUID().toString();
            log.info("uniqueId on the partition {}", uniqueId);
            while (ite1.hasNext() && ite2.hasNext()) {
                list.add(uniqueId + " " + ite1.next() + " " + ite2.next());
            }
            return list.iterator();
        });

        /*JavaRDD<String> javaZipRDD = firstL.zipPartitions(secondl, (ite1, ite2) -> {
            List<String> list = new ArrayList();
            String uniqueId = UUID.randomUUID().toString();
            log.info("uniqueId on the partition {}", uniqueId);
            while (ite1.hasNext() && ite2.hasNext()) {
                list.add(uniqueId + " " + ite1.next() + " " + ite2.next());
            }
            return list.iterator();
        });*/

   //     javaZipRDD.foreach(val -> log.info("value {}", val));


         firstL = sc.parallelize(IntStream.rangeClosed(1, 10).mapToObj(String::valueOf).collect(Collectors.toList()), 4);
         secondl = sc.parallelize(IntStream.rangeClosed(11, 17).mapToObj(String::valueOf).collect(Collectors.toList()), 4);

        firstL.zipPartitions(secondl, (ite1, ite2) -> {
                List<String> list = new ArrayList();
                String uniqueId = UUID.randomUUID().toString();
                log.info("uniqueId on the partition {}", uniqueId);
                while (ite1.hasNext() && ite2.hasNext()) {
                    list.add(uniqueId + " " + ite1.next() + " " + ite2.next());
                } while (ite1.hasNext()) {
                    list.add(uniqueId + " " + ite1.next() + ", EMPTY ");
                }while (ite2.hasNext()) {
                    list.add(uniqueId + " EMPTY, " + ite2.next());
                }
                return list.iterator();
            }).foreach(val-> log.info("second zip res {}", val));
        Utils.sleep(100000);
    }
}
