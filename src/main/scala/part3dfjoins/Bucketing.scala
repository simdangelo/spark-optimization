package part3dfjoins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Bucketing {

  val spark = SparkSession.builder()
    .appName("Bucketing")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")
  joined.explain() // classical join


  // BUCKETING (for join)
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small")

  val bucketedLarge = spark.table("bucketed_large")
  val bucketedSmall = spark.table("bucketed_small")
  val bucketedJoin = bucketedLarge.join(bucketedSmall, "id")
  bucketedJoin.explain()

  joined.count() // 3.5s
  bucketedJoin.count() // 3s, but bucketing is one time cost. the subsequent join are way faster than joined.count()


  // BUCKETING (for grouping)
  val flightDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights/flights.json")
    .repartition(2)

  val mostDelayed = flightDF
    .filter("origin = 'DEN' and arrdelay>1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)
  mostDelayed.explain()

  flightDF.write
    .partitionBy("origin")
    .bucketBy(4, "dest", "carrier")
    .saveAsTable("flights_bucketed") // just as long as shuffle

  val flightsBucketed = spark.table("flights_bucketed")
  val mostDelayed2 = flightsBucketed
    .filter("origin = 'DEN' and arrdelay>1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)
  mostDelayed2.explain()

  mostDelayed.show()
  mostDelayed2.show()

  println("flightDF partition number: ", flightDF.rdd.getNumPartitions)
  println("flightsBucketed partition number: ", flightsBucketed.rdd.getNumPartitions)
  println("mostDelayed2 partition number: ", mostDelayed2.rdd.getNumPartitions)



  // BUCKETING PRUNING
  val the10 = bucketedLarge.filter($"id"===10) // of course it will have 1 row
  the10.explain()
  the10.show()


  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}
