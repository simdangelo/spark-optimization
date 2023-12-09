package part3dfjoins

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SkewedJoins {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  /*
  An online store selling gaming laptops
  2 laptops are "similar" if they have the same make&model, but proc speed within 0.1ghz

  For each laptop configuration, we are interested in the average sale price of "similar model"

  Acer Predator 2.9Ghz avlfeoinqw -> average sale price of all Acer Predators with CPU speed between 2.8Ghz and 3.0Ghz
   */

  val laptops = Seq.fill(40000)(DataGenerator.randomLaptop()).toDS
  val laptopsOffer = Seq.fill(100000)(DataGenerator.randomLaptopOffer()).toDS

  val joined = laptops.join(laptopsOffer, Seq("make", "model"))
    .filter(abs(laptopsOffer.col("procSpeed") - laptops.col("procSpeed")) <= 0.1)
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  laptops.show()
  laptopsOffer.show()
  joined.show()
  joined.explain()


  // Solution for DATA SKEW problem
  val laptops2 = laptops.withColumn("procSpeed", explode(array($"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1)))
  laptops2.show()
  val joined2 = laptops.join(laptopsOffer, Seq("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}
