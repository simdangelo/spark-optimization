package part3dfjoins

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PrePartitioning {

  val spark = SparkSession.builder()
    .appName("Pre-partitioning")
    .config("spark.sql.adaptive.enabled", "false")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  /*
  addColumnd(initiaTable, 3) => dataframe with columns "id", "newCol1", "newCol2", "newCol3",
   */
  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr("id" +: newColumns: _*)
  }

  val initialTable = spark.range(1, 10000000).repartition(10)
  val narrowTable = spark.range(1, 5000000).repartition(7)

  // scenario 1
  val wideTable = addColumns(initialTable, 30)
  val join1 = wideTable.join(narrowTable, "id")
//  join1.explain()
//  println(join1.show())

  // scenario 2
  val altNarrow = narrowTable.repartition($"id")
  val altInitial = initialTable.repartition($"id")
  val join2 = altInitial.join(altNarrow, "id")
  val result2 = addColumns(join2, 30)
//  result2.explain()
//  println(result2.show())


  // SCENARIO 3
  val enhanceColumnsFirst = addColumns(initialTable, 30)
  val repartitionedNarrow = narrowTable.repartition($"id")
  val repartitionedEnhance = enhanceColumnsFirst.repartition($"id")
  val result3 = repartitionedEnhance.join(repartitionedNarrow, "id")
  result3.show()
  result3.explain()

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
