import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object PogodaETL {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("PogodaETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val weatherDS = spark.read.text(args(0) + "/weather.txt").cache()
    val weather = weatherDS
      .map(row => row
        .getString(0)
        .split(": ", 2)(1)
      )
      .distinct()
      .withColumnRenamed("value", "opis_pogody")

    val weatherWithIndex = spark.sqlContext.createDataFrame(
      weather.rdd.zipWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ (index + 1))
      },
      StructType(weather.schema.fields :+ StructField("id_pogody", LongType, false))
    )

    weatherWithIndex
      .select("id_pogody", "opis_pogody")
      .write.format("delta").saveAsTable("w_pogoda")

    println("Załadowano dane do tabeli wymiaru 'w_pogoda'")
  }
}
