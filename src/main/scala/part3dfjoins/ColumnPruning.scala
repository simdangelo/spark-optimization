package part3dfjoins

import org.apache.spark.sql.SparkSession

object ColumnPruning {

  val spark = SparkSession.builder()
    .appName("Column Pruning")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.implicits._

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars/guitars.json")

  val guitarPlayersDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands/bands.json")

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")

  val guitaristsBandsDF = guitarPlayersDF.join(bandsDF, joinCondition)
  guitaristsBandsDF.explain(true)
  guitaristsBandsDF.show


  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
