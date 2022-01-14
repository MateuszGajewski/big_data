import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object PogodaETL {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("KategoriaDrogiETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val scotlandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataScotland.csv")

    val northEnglandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataNorthEngland.csv")

    val southEnglandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataSouthEngland.csv")

    val allDataDF = scotlandMainDS.union(northEnglandMainDS).union(southEnglandMainDS)
      .select($"road_category", $"road_type")
      .distinct()
      .withColumnRenamed("road_category", "kategoria_drogi")
      .withColumnRenamed("road_type", "typ_drogi")

    val window = Window.orderBy($"kategoria_drogi")
    val finalDataDF = allDataDF.withColumn("id_kat_drogi", row_number.over(window))
      .select("id_kat_drogi", "kategoria_drogi", "typ_drogi")

    finalDataDF.write.format("delta").mode("overwrite").saveAsTable("w_kategoria_drogi")

    println("Załadowano dane do tabeli wymiaru 'w_kategoria_drogi'")
  }
}
