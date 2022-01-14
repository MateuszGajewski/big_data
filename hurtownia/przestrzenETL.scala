import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PogodaETL {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("PrzestrzenETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val scotlandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataScotland.csv")

    val scotlandRegionDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/regionsScotland.csv")

    val scotlandAuthorityDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/authoritiesScotland.csv")

    val northEnglandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataNorthEngland.csv")

    val northEnglandRegionDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/regionsNorthEngland.csv")

    val northEnglandAuthorityDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/authoritiesNorthEngland.csv")

    val southEnglandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/mainDataSouthEngland.csv")

    val southEnglandRegionDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/regionsSouthEngland.csv")

    val southEnglandAuthorityDS = spark.read.format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .csv(args(0) + "/authoritiesSouthEngland.csv")

    val scotlandAll = scotlandMainDS.join(scotlandAuthorityDS, scotlandMainDS("local_authoirty_ons_code").equalTo(scotlandAuthorityDS("local_authority_ons_code")), "leftouter")
      .join(scotlandRegionDS, scotlandAuthorityDS("region_ons_code").equalTo(scotlandRegionDS("region_ons_code")), "leftouter")
      .select($"region_name", $"local_authority_name", $"local_authoirty_ons_code", $"road_name")

    val northEnglandAll = northEnglandMainDS.join(northEnglandAuthorityDS, northEnglandMainDS("local_authoirty_ons_code").equalTo(northEnglandAuthorityDS("local_authority_ons_code")), "leftouter")
      .join(northEnglandRegionDS, northEnglandAuthorityDS("region_ons_code").equalTo(northEnglandRegionDS("region_ons_code")), "leftouter")
      .select($"region_name", $"local_authority_name", $"local_authoirty_ons_code", $"road_name")

    val southEnglandAll = southEnglandMainDS.join(southEnglandAuthorityDS, southEnglandMainDS("local_authoirty_ons_code").equalTo(southEnglandAuthorityDS("local_authority_ons_code")), "leftouter")
      .join(southEnglandRegionDS, southEnglandAuthorityDS("region_ons_code").equalTo(southEnglandRegionDS("region_ons_code")), "leftouter")
      .select($"region_name", $"local_authority_name", $"local_authoirty_ons_code", $"road_name")


    val allDataDF = scotlandAll.union(northEnglandAll).union(southEnglandAll)
      .distinct()
      .withColumnRenamed("region_name", "nazwa_regionu")
      .withColumnRenamed("local_authority_name", "nazwa_obszaru_adm")
      .withColumnRenamed("local_authoirty_ons_code", "kod_obszaru_adm")
      .withColumnRenamed("road_name", "nazwa_drogi")
      .withColumn("id_przestrzen", monotonically_increasing_id)
      .select("id_przestrzen", "nazwa_regionu", "nazwa_obszaru_adm", "kod_obszaru_adm", "nazwa_drogi")

    val window = Window.orderBy($"id_przestrzen")

    val finalDataDF = allDataDF.withColumn("id_przestrzen", row_number.over(window))

    finalDataDF.write.format("delta").mode("overwrite").saveAsTable("w_przestrzen")

    println("Załadowano dane do tabeli wymiaru 'w_przestrzen'")
  }
}
