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
public class Employees {

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

    public void setDepartment(String department) {
      this.department = department;
    }

    public String getDesignation() {
      return designation;
    }

    public void setDesignation(String designation) {
      this.designation = designation;
    }

    public long getCostToCompany() {
      return costToCompany;
    }

    public void setCostToCompany(long costToCompany) {
      this.costToCompany = costToCompany;
    }

    public String getState() {
      return state;
    }

    public void setState(String state) {
      this.state = state;
    }




    // constructor , getters and setters
  }
  public static void main(String[] args) {


    JavaSparkContext sc = getSparkContext();

    AtomicInteger atomicInteger = new AtomicInteger();
    org.apache.spark.Accumulator<Integer> partitionAcc = sc.accumulator(0,"partition");

    Broadcast<AtomicInteger> broadcast = sc.broadcast(atomicInteger);
    JavaRDD<String> data = sc.textFile("in/employee.csv");

    JavaRDD<Record> rdd_records = data
        .filter(line-> StringUtils.isNumeric(line.split(",")[2].trim()))
        .map(
        (Function<String, Record>) line -> {
          String[] fields = line.split(",");
          Record sd = new Record(fields[0], fields[1], Long.valueOf(fields[2].trim()), fields[3]);
          return sd;
        });




    JavaPairRDD<String, Tuple2<Long, Integer>> records_JPRDD =
        rdd_records.mapToPair((PairFunction<Record, String, Tuple2<Long, Integer>>) record -> {
          Tuple2<String, Tuple2<Long, Integer>> t2 =
              new Tuple2<>(
                  String.join(",", record.getDepartment().trim() , record.getDesignation().trim() , record.getState().trim()),
                  new Tuple2<>(record.costToCompany, 1)
              );
          return t2;
        });
    JavaPairRDD<String, Tuple2<Long, Integer>> final_rdd_records =
        records_JPRDD.reduceByKey(
            (tup1, tup2) -> new Tuple2<>(tup1._1 + tup2._1, tup1._2 + tup2._2));
    JavaRDD<String> finalOutputRdd = final_rdd_records.map(t -> String.join(",", t._1, String.valueOf(t._2._1), String.valueOf(t._2._2)));
   // final_rdd_records.foreach(tup-> log.info("{} : {}", tup._1, tup._2 ));

    finalOutputRdd.foreach(m-> log.info(m));
    log.info("number of records {}", finalOutputRdd.count());
    finalOutputRdd.foreachPartition(stringIterator -> {
      int nextInt = broadcast.getValue().incrementAndGet();
      log.info("iterating by partition {}", nextInt);
      while (stringIterator.hasNext()) {
        log.info("inside foreach partition {} and value {} and thread {}", nextInt, stringIterator.next(), Thread.currentThread().getName());
      }
    });
    partitionAcc.add(1);
    finalOutputRdd.saveAsTextFile("in/emp_output-"+ partitionAcc.value());

    sc.close();
 //   counts.saveAsTextFile("data_counts");

  }



}
