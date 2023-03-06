package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Hello world! */
@Slf4j
public class GroupBy {

  public static void main(String[] args) {
    System.setProperty("spark.master", "local");

    SparkSession.Builder builder = SparkSession.builder();
    builder.config("spark.sql.adaptive.coalescePartitions.parallelismFirst", true);
    builder.config("spark.sql.files.minPartitionNum", 2);
    SparkSession session = builder.appName("my App").getOrCreate();

    try (JavaSparkContext sc = new JavaSparkContext(session.sparkContext())) {

      JavaRDD<Emp> first =
          sc.parallelize(
              IntStream.rangeClosed(1, 6)
                  .mapToObj(x -> new Emp(x, "first:" + x))
                  .collect(Collectors.toList()));
      JavaRDD<Emp> second =
          sc.parallelize(
              IntStream.rangeClosed(5, 10)
                  .mapToObj(x -> new Emp(x, "second:" + x))
                  .collect(Collectors.toList()));
      JavaRDD<Emp> third =
          sc.parallelize(
              IntStream.rangeClosed(4, 6)
                  .mapToObj(x -> new Emp(x, "third:" + x))
                  .collect(Collectors.toList()));

      List<JavaPairRDD<Integer, Emp>> list = new ArrayList<>();
      JavaPairRDD<Integer, Emp> fp = first.mapToPair(emp -> new Tuple2<>(emp.getId(), emp));
      JavaPairRDD<Integer, Emp> sp = second.mapToPair(emp -> new Tuple2<>(emp.getId(), emp));
      JavaPairRDD<Integer, Emp> tp = third.mapToPair(emp -> new Tuple2<>(emp.getId(), emp));
      list.add(fp);
      list.add(sp);
      list.add(tp);

      JavaPairRDD<Integer, Emp> allEmps = null;

      for (JavaPairRDD<Integer, Emp> l : list) {
        if (allEmps == null) {
          allEmps = l;
        } else allEmps = allEmps.union(l);
      }
      JavaPairRDD<Integer, Iterable<Emp>> pairRdd = allEmps.groupByKey();

      JavaRDD<String> resultRdd = pairRdd.mapPartitions(
          ite -> {
            StringWriter sw = new StringWriter();
            try (CSVPrinter writer = new CSVPrinter(sw, CSVFormat.DEFAULT)) {
              writer.printRecord("Id", "Name");
              while (ite.hasNext()) {
                Tuple2<Integer, Iterable<Emp>> row = ite.next();
                Iterator<Emp> iterator = row._2.iterator();
                while (iterator.hasNext()) {
                  writer.printRecord(iterator.next().getRow());
                }
              }
            }
            return Arrays.asList(sw.toString()).iterator();
          });

      resultRdd.saveAsTextFile("in/csv-"+ UUID.randomUUID());
      while (true) {
        Thread.sleep(5000);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
