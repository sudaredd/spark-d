package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.example.Utils.getSparkContext;

/** Hello world! */
@Slf4j
public class ReadMultipleFileEmployees {

  static class Record implements Serializable {
    String department;
    String designation;
    long costToCompany;
    String state;

    public Record(String department, String designation, long costToCompany, String state) {
      this.department = department;
      this.designation = designation;
      this.costToCompany = costToCompany;
      this.state = state;
    }

    public String getDepartment() {
      return department;
    }

    public String getDesignation() {
      return designation;
    }

    public String getState() {
      return state;
    }

    @Override
    public String toString() {
      return "Record{" +
          "department='" + department + '\'' +
          ", designation='" + designation + '\'' +
          ", costToCompany=" + costToCompany +
          ", state='" + state + '\'' +
          '}';
    }
  }

  public static void main(String[] args) {

    JavaSparkContext sc = getSparkContext();

    JavaRDD<String> data = sc.textFile("in/files/");

    JavaRDD<Record> rdd_records =
        data.filter(line -> StringUtils.isNumeric(line.split(",")[2].trim()))
            .map(
                (Function<String, Record>)
                    line -> {
                      String[] fields = line.split(",");
                      Record sd =
                          new Record(
                              fields[0], fields[1], Long.valueOf(fields[2].trim()), fields[3]);
                      return sd;
                    });
    rdd_records.foreachPartition(
        iterator -> {
          while (iterator.hasNext()) {
            log.info("Emp {}", iterator.next());
          }
        });

    sc.close();
    //   counts.saveAsTextFile("data_counts");

  }
}
