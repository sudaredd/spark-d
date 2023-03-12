package org.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

import static org.example.Utils.getSparkContext;

@Slf4j
public class CompareFiles {

  @AllArgsConstructor
  @NoArgsConstructor
  @Data@ToString
  @EqualsAndHashCode
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

    JavaPairRDD<String, Iterable<Emp>> existingRows = getRows(existing);

    JavaPairRDD<String, Iterable<Emp>> newRows = getRows(new_);

    JavaPairRDD<String, Tuple2<Optional<Iterable<Emp>>, Optional<Iterable<Emp>>>> joinRows = existingRows.fullOuterJoin(newRows);

    JavaRDD<ReportRecord> reports =
        joinRows
            .map(
                tup -> getReportRecord(tup))
            .filter(record -> !ReportRecord.EMPTY.equals(record));
    reports.foreach(r-> log.info("differences in a record {}", r));

    sc.close();
  }

  private static ReportRecord getReportRecord(Tuple2<String, Tuple2<Optional<Iterable<Emp>>, Optional<Iterable<Emp>>>> tup) {
    Optional<Iterable<Emp>> iterableOptional1 = tup._2._1;
    Optional<Iterable<Emp>> iterableOptional2 = tup._2._2;
    if (iterableOptional1.isPresent() && iterableOptional2.isPresent()) {
      if (iterableOptional1.get().equals(iterableOptional2.get())) {
        log.info("emp objects are equals for id {}", iterableOptional1.get());
        return ReportRecord.EMPTY;
      } else {
        log.info(
            "emp objects are not equals for id {} => {}",
            iterableOptional1.get(),
            iterableOptional2.get());
        return new ReportRecord(ReportEnum.NOT_EQUAL, Source.NEW, tup._1);
      }
    } else if (!iterableOptional1.isPresent()) {
      log.info("No row exists for empId {} on existing", tup._1);
      return new ReportRecord(ReportEnum.MISSING, Source.EXISTING, tup._1);
    } else {
      log.info("No row exists for empId {} on a new", tup._1);
      return new ReportRecord(ReportEnum.MISSING, Source.NEW, tup._1);
    }
  }

  private static JavaPairRDD<String, Iterable<Emp>> getRows(JavaRDD<String[]> existing) {
    return existing.mapToPair(strings -> new Tuple2<>(strings[0], Emp.newInstance(strings)))
        .groupByKey();
  }
  private static JavaRDD<String[]> getJavaRDD(JavaRDD<String> data) {
    return data.map(s -> s.split(","))
        .filter(line -> StringUtils.isNumeric(line[0].trim()));
  }
}
