package org.example;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.directory.shared.kerberos.codec.kdcReqBody.actions.StoreRTime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class Emp implements Serializable {

  int id;
  private String name;

  public Emp() {}

  public Emp(int id, String name) {
    this.id = id;
    this.name = name;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public List<String> getRow() {
    return Arrays.asList(String.valueOf(getId()), getName());
  }

  @Override
  public String toString() {
    return "Emp{" + "dept=" + ", id=" + id + ", name='" + name + '\'' + '}';
  }
}

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
class DeptImpl implements Dept, Serializable {
  String deptName;
}
@AllArgsConstructor
class SecondDeptImpl extends DeptImpl implements Serializable {
  public SecondDeptImpl(String deptName) {
    super(deptName);
  }

  @Override
  public String toString() {
    return "SecondDeptImpl{" +
        "deptName='" + deptName + '\'' +
        '}';
  }
}

interface Dept {

}
/** Hello world! */
@Slf4j
public class NestedJavaRdd {

  public static void main(String[] args) {
    System.setProperty("spark.master","local");

    SparkSession.Builder builder = SparkSession.builder();
    builder.config("spark.sql.adaptive.coalescePartitions.parallelismFirst", true);
    builder.config("spark.sql.files.minPartitionNum", 2);
    SparkSession session = builder.appName("my App").getOrCreate();

    try (JavaSparkContext sc = new JavaSparkContext(session.sparkContext())) {

      JavaRDD<Integer> input = sc.parallelize(IntStream.rangeClosed(1, 6).mapToObj(a->a).collect(Collectors.toList()));
      JavaRDD<Integer> second = sc.parallelize(IntStream.rangeClosed(5, 10).mapToObj(a->a).collect(Collectors.toList()));


      JavaPairRDD<Integer, Emp> firstWords = input.mapPartitionsWithIndex((partId, partition) -> {
        List<Emp> l = new ArrayList<>();
        while (partition.hasNext()) {
          int x = partition.next();
          l.add(new Emp(x, String.valueOf(x)));
          log.info("partitionId {}", partId);
        }
        return l.iterator();
      }, false).mapPartitionsToPair(iterator -> {
        List<Tuple2<Integer, Emp>> l = new ArrayList<>();
        while (iterator.hasNext()) {
          Emp emp = iterator.next();
          emp.setName(emp.getName() + " first");
          l.add(new Tuple2<>(emp.getId(), emp));
        }
        return l.iterator();
      });
      JavaPairRDD<Integer, Emp> secondWords = second.mapPartitionsWithIndex((partId, partition) -> {
        List<Emp> l = new ArrayList<>();
        while (partition.hasNext()) {
          int x = partition.next();
          l.add(new Emp(x, String.valueOf(x)));
          log.info("partitionId {}", partId);
        }
        return l.iterator();
      }, false).mapPartitionsToPair(iterator -> {
        List<Tuple2<Integer, Emp>> l = new ArrayList<>();
        while (iterator.hasNext()) {
          Emp emp = iterator.next();
          emp.setName(emp.getName() + " second");
          l.add(new Tuple2<>(emp.getId(), emp));
        }
        return l.iterator();
      });

      JavaPairRDD<Integer, Tuple2<Optional<Emp>, Optional<Emp>>> join = firstWords.fullOuterJoin(secondWords);

      join.foreach(tup-> {
        log.info("id=> {}, tup1=> {}, tup2=> {}", tup._1, tup._2._1, tup._2._2);
      });
      System.out.printf("completed");
    }
  }
}
