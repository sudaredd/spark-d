package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.execution.columnar.ARRAY;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.example.Utils.getSparkContext;

@Slf4j
public class CompareFiles {

  @AllArgsConstructor
  @NoArgsConstructor
  @Data@ToString
  private static class Emp implements Serializable{
    private int id;
    private String name;

    public static Emp newInstance(String []row) {
      return new Emp(Integer.parseInt(row[0]), row[1]);
    }

  }
  public static void main(String[] args) {

    JavaSparkContext sc = getSparkContext();


    JavaRDD<String[]> existing = getJavaRDD(sc.textFile("in/existing/"));
    JavaRDD<String[]> new_ = getJavaRDD(sc.textFile("in/new/"));

    JavaPairRDD<String, Iterable<Emp>> existingRows = existing.mapToPair(strings -> new Tuple2<>(strings[0], Emp.newInstance(strings)))
        .groupByKey();

    JavaPairRDD<String, Iterable<Emp>> newRows = new_.mapToPair(strings -> new Tuple2<>(strings[0], Emp.newInstance(strings)))
        .groupByKey();

    JavaPairRDD<String, Tuple2<Optional<Iterable<Emp>>, Optional<Iterable<Emp>>>> joinRows = existingRows.fullOuterJoin(newRows);

    joinRows.foreach(tup-> {
      Optional<Iterable<Emp>> iterableOptional1 = tup._2._1;
      if (iterableOptional1.isPresent()) {
        log.info("row {} and it's value {}", tup._1, iterableOptional1.get());
      } else {
        log.info("No row exists for empId {} on existing", tup._1);
      }
      Optional<Iterable<Emp>> iterableOptional2 = tup._2._2;
      if (iterableOptional2.isPresent()) {
        log.info("row {} and it's value {}", tup._1, iterableOptional2.get());
      } else {
        log.info("No row exists for empId {} on new", tup._1);
      }

    });

    sc.close();
    //   counts.saveAsTextFile("data_counts");

  }

  private static void print(JavaRDD<String[]> existing, String fileTyp) {
    existing.foreachPartition(
        iterator -> {
          log.info("fileTyp {}", fileTyp);
          while (iterator.hasNext()) {
            log.info("row {}", Arrays.toString(iterator.next()));
          }
        });
  }

  private static JavaRDD<String[]> getJavaRDD(JavaRDD<String> data) {
    return data.map(s -> s.split(","))
        .filter(line -> StringUtils.isNumeric(line[0].trim()));
  }
}
