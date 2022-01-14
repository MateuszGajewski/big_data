import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession
import scala.collection.mutable

case class Typ_Pojazdu(id_typ: Int, typ_pojazdu: String, kategoria: String, silnikowy: Boolean, dwu_kolowy: Boolean)

object PogodaETL {

  def main(args: Array[String]) {

    val categories = Map(
      "all_motor_vehicles" -> List("All Motor Vehicles", "All_MV"),
      "two_wheeled_motor_vehicles" -> List("Two-wheeled motor vehicles", "2WMV"),
      "cars_and_taxis" -> List("Cars and Taxis", "Car"),
      "lgvs" -> List("Light Goods Vans", "LGV"),
      "buses_and_coaches" -> List("Buses and coaches", "BC"),
      "all_hgvs" -> List("Heavy Goods Vehicle total", "HGV"),
      "hgvs_2_rigid_axle" -> List("2-rigid axle Heavy Goods Vehicle", "HGV"),
      "hgvs_3_rigid_axle" -> List("3-rigid axle Heavy Goods Vehicle", "HGV"),
      "hgvs_4_or_more_rigid_axle" -> List("4 or more rigid axle Heavy Goods Vehicle", "HGV"),
      "hgvs_3_or_4_articulated_axle" -> List("3 and 4-articulated axle Heavy Goods Vehicle", "HGV"),
      "hgvs_5_articulated_axle" -> List("5-articulated axle Heavy Goods Vehicle", "HGV"),
      "hgvs_6_articulated_axle" -> List("6 or more articulated axle Heavy Goods Vehicle", "HGV"),
      "pedal_cycles" -> List("Pedal Cycles", "PC")
    )

    val spark = SparkSession.builder()
      .appName("TypPojazduETL")
      //.master("local")
      .enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    val scotlandMainDS = spark.read.format("org.apache.spark.csv")
      .option("header", false)
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

    val vehicles = mutable.MutableList[Typ_Pojazdu]()
    var id_pojazdu: Int = 1
    val header = allDataDF.first().toSeq.toList.takeRight(13)

    header.foreach(p => {
      vehicles += Typ_Pojazdu(id_pojazdu,  p.toString, categories(p.toString).head, if("pedal_cycles".equals(p)) false else true,
        if("pedal_cycles".equals(p) | "two_wheeled_motor_vehicles".equals(p)) true else false)
      id_pojazdu += 1
    })

    val typPojazduDS = vehicles.toDS()

    typPojazduDS.write.format("delta").mode("overwrite").saveAsTable("w_typ_pojazdu")

    println("Załadowano dane do tabeli wymiaru 'w_typ_pojazdu'")
  }
}
