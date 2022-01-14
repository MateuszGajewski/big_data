
import java.util.Calendar

import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StructField, StructType, IntegerType}

object PogodaETL {
  def getSeason(month: Int): Long = month match {
    case (2 | 1 | 11 | 12) =>
      4
    case 3 | 4=>
      1
    case 5 | 6 | 7 | 8 =>
      2
    case _ =>
      3
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("CzasETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

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

    val time = scotlandMainDS
      .union(northEnglandMainDS)
      .union(southEnglandMainDS)
      .select("count_date", "hour")
      .distinct()

    val timeWithIndex = spark.sqlContext.createDataFrame(
      time.rdd.zipWithIndex.map {
        case (row, index) =>
          val calendar = Calendar.getInstance

          calendar.setTime(row.getTimestamp(0))

          val month = (calendar.get(Calendar.MONTH) + 1).toLong
          var dayOfWeek = (calendar.get(Calendar.DAY_OF_WEEK) - 1).toLong

          if (dayOfWeek == 0) {
            dayOfWeek = 7
          }


          Row.fromSeq(row.toSeq :+
            (index + 1) :+
            calendar.get(Calendar.YEAR).toLong :+
            month :+
            (getSeason(month.toInt)) :+
            dayOfWeek
          )
      },
      StructType(time.schema.fields :+
        StructField("id_czas", LongType, false) :+
        StructField("rok", LongType, false) :+
        StructField("miesiac", LongType, false) :+
        StructField("season", LongType, false) :+
        StructField("dzien_tygodnia", LongType, false)
      )
    )

    timeWithIndex
      .withColumnRenamed("count_date", "data")
      .withColumnRenamed("hour", "godzina")
      .select("id_czas", "data", "rok", "miesiac", "godzina", "season", "dzien_tygodnia")
      .write.format("delta").mode("overwrite").saveAsTable("w_czas")

    println("Załadowano dane do tabeli wymiaru 'w_czas'")
  }
}
