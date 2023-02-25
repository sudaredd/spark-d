package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Hello world! */
@Slf4j
public class GroupBy {

  public static void main(String[] args) {
    System.setProperty("spark.master","local");

    SparkSession.Builder builder = SparkSession.builder();
    builder.config("spark.sql.adaptive.coalescePartitions.parallelismFirst", true);
    builder.config("spark.sql.files.minPartitionNum", 2);
    SparkSession session = builder.appName("my App").getOrCreate();

    try (JavaSparkContext sc = new JavaSparkContext(session.sparkContext())) {

      JavaRDD<Emp> first = sc.parallelize(IntStream.rangeClosed(1, 6).mapToObj(x->new Emp(x, "first:"+String.valueOf(x))).collect(Collectors.toList()));
      JavaRDD<Emp> second = sc.parallelize(IntStream.rangeClosed(5, 10).mapToObj(x->new Emp(x, "second:" + String.valueOf(x))).collect(Collectors.toList()));
      JavaRDD<Emp> third = sc.parallelize(IntStream.rangeClosed(4, 6).mapToObj(x->new Emp(x, "third:"+String.valueOf(x))).collect(Collectors.toList()));

      List<JavaPairRDD<Integer, Emp>> list = new ArrayList<>();
      JavaPairRDD<Integer, Emp> fp = first.mapToPair(emp-> new Tuple2<>(emp.getId(), emp));
      JavaPairRDD<Integer, Emp> sp = second.mapToPair(emp-> new Tuple2<>(emp.getId(), emp));
      JavaPairRDD<Integer, Emp> tp = third.mapToPair(emp-> new Tuple2<>(emp.getId(), emp));
      list.add(fp);
      list.add(sp);
      list.add(tp);

      JavaPairRDD<Integer, Emp> allEmps = null;

      for (JavaPairRDD<Integer, Emp> l: list) {
        if (allEmps == null) {
          allEmps = l;
        } else
          allEmps = allEmps.union(l);
      }
      JavaPairRDD<Integer, Iterable<Emp>> pairRdd = allEmps.groupByKey();
      pairRdd.foreach(tup-> {
        log.info("row key {} and iterator {}", tup._1, tup._2);
      });

    }
  }
}
