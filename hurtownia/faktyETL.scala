import org.apache.spark.sql._
import org.apache.spark.sql.functions.{date_format, split, to_date, unix_timestamp}

object PogodaETL {

  def getPattern(line: String): String = {
    val pattern = "^In the region of (.+) on (.+) at (.+):.+ the following weather conditions were reported: (.+)$".r
    line match {
      case pattern(auth, date, hour, conditions) => auth + ";" + date + ";" + hour.toInt + ";" + conditions
      case _ => ""
    }
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("FaktyETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val scotlandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataScotland.csv")
      .cache()

    val northEnglandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataNorthEngland.csv")
      .cache()

    val southEnglandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataSouthEngland.csv")
      .cache()

    val allDataDF = scotlandMainDS
      .union(northEnglandMainDS)
      .union(southEnglandMainDS)
      .selectExpr("year", "count_date", "hour", "local_authoirty_ons_code", "road_name", "road_category",
        "road_type", "stack(13, 'pedal_cycles', pedal_cycles, 'two_wheeled_motor_vehicles', two_wheeled_motor_vehicles, " +
          "'cars_and_taxis', cars_and_taxis, 'buses_and_coaches', buses_and_coaches, 'lgvs', lgvs, " +
          "'hgvs_2_rigid_axle', hgvs_2_rigid_axle, 'hgvs_3_rigid_axle', hgvs_3_rigid_axle, " +
          "'hgvs_4_or_more_rigid_axle', hgvs_4_or_more_rigid_axle, 'hgvs_3_or_4_articulated_axle', hgvs_3_or_4_articulated_axle, " +
          "'hgvs_5_articulated_axle', hgvs_5_articulated_axle, 'hgvs_6_articulated_axle', hgvs_6_articulated_axle, " +
          "'all_hgvs', all_hgvs, 'all_motor_vehicles', all_motor_vehicles) as (pojazd, liczba_pojazdow)")
      .withColumn("count_date", date_format($"count_date", "dd/MM/yyyy"))


    val katDrogiDF = spark.sql("Select * from w_kategoria_drogi")
    val przestrzenDF = spark.sql("Select * from w_przestrzen")
    val weatherDS = spark.sql("Select * from w_pogoda")
    val czasDS = spark.sql("Select * from w_czas")
    val typPojazduDS = spark.sql("Select * from w_typ_pojazdu")
    val weatherFromFile = spark.read.textFile(args(0) + "/weather.txt").map(line => getPattern(line)).filter(!_.equals(""))
      .withColumn("_tmp", split($"value", ";"))
      .select($"_tmp".getItem(0).as("kod_obszaru_adm"),
        $"_tmp".getItem(1).as("data"),
        $"_tmp".getItem(2).as("godzina"),
        $"_tmp".getItem(3).as("pogoda"))
      .distinct()

    val afterJoinWithCatDF = allDataDF.join(katDrogiDF, allDataDF("road_category").equalTo(katDrogiDF("kategoria_drogi")))
      .drop("road_category", "road_type", "kategoria_drogi", "typ_drogi")

    val afterJoinWithPrzeDF = afterJoinWithCatDF.join(przestrzenDF, afterJoinWithCatDF("local_authoirty_ons_code").equalTo(przestrzenDF("kod_obszaru_adm")) &&
      afterJoinWithCatDF("road_name").equalTo(przestrzenDF("nazwa_drogi")))
      .drop("road_name", "nazwa_obszaru_adm", "nazwa_regionu", "nazwa_drogi", "kod_obszaru_adm")

    val afterJoinWithTextFileDF = afterJoinWithPrzeDF.join(weatherFromFile, afterJoinWithPrzeDF("local_authoirty_ons_code").equalTo(weatherFromFile("kod_obszaru_adm")) &&
      afterJoinWithPrzeDF("hour").equalTo(weatherFromFile("godzina")) && afterJoinWithPrzeDF("count_date").equalTo(weatherFromFile("data")))
      .drop("nazwa_obszaru_adm", "data", "godzina", "local_authoirty_ons_code", "kod_obszaru_adm")
      .withColumn("count_date", date_format(unix_timestamp($"count_date", "dd/MM/yyyy").cast("timestamp"), "yyyy-MM-dd"))

    val afterJoinWithPogDF = afterJoinWithTextFileDF.join(weatherDS, afterJoinWithTextFileDF("pogoda").equalTo(weatherDS("opis_pogody")))
      .drop("opis_pogody", "pogoda")

    val afterJoinWithCzasDF = afterJoinWithPogDF.join(czasDS, afterJoinWithPogDF("count_date").equalTo(czasDS("data")) &&
      afterJoinWithPogDF("hour").equalTo(czasDS("godzina")))
      .drop("dzien_tygodnia", "kwartal", "godzina", "miesiac", "rok", "data", "count_date", "year", "hour")

    val afterJoinWithTypDF = afterJoinWithCzasDF.join(typPojazduDS, afterJoinWithCzasDF("pojazd").equalTo(typPojazduDS("typ_pojazdu")))
      .drop("kategoria", "silnikowy", "pojazd", "typ_pojazdu")

    val finalDataDF = afterJoinWithTypDF.withColumnRenamed("id_typ", "id_typ_pojazdu")
      .select("id_pogody", "id_przestrzen", "id_kat_drogi", "id_czas", "id_typ_pojazdu", "liczba_pojazdow")

    finalDataDF.write.format("delta").mode("overwrite").saveAsTable("f_fakty")

    println("Załadowano dane do tabeli wymiaru 'f_fakty'")
  }
}
