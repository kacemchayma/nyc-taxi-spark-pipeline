import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object TaxiPhase3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Phase 3 Spatio-Temporelle (Bonus)")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =====================================================
    // LECTURE DES DONNEES TAXI
    // =====================================================
    val taxiPath =
      "data/yellow_parquet/yellow_tripdata_2025-03.parquet"

    val df = spark.read.parquet(taxiPath)
      .withColumn("pickup_ts", col("tpep_pickup_datetime").cast("timestamp"))
      .filter(col("pickup_ts").isNotNull)
      .withColumn("hour", hour(col("pickup_ts")))
      .withColumn("day_of_week", date_format(col("pickup_ts"), "E"))

    println(s"Nombre total de trajets charges : ${df.count()}")

    // =====================================================
    // LECTURE DU FICHIER DE CORRESPONDANCE DES ZONES
    // =====================================================
    val zoneLookup = spark.read
      .option("header", "true")
      .csv("data/taxi_zone_lookup.csv")
      .select(
        col("LocationID").cast("int"),
        col("Borough"),
        col("Zone")
      )

    // =====================================================
    // 1. TOP ZONES DE PICKUP & DROPOFF
    // =====================================================
    val topPickupZones = df.groupBy("PULocationID")
      .count()
      .orderBy(desc("count"))
      .limit(10)
      .join(
        zoneLookup,
        col("PULocationID") === zoneLookup("LocationID"),
        "left"
      )
      .select(
        col("PULocationID"),
        col("Zone"),
        col("Borough"),
        col("count")
      )

    val topDropoffZones = df.groupBy("DOLocationID")
      .count()
      .orderBy(desc("count"))
      .limit(10)
      .join(
        zoneLookup,
        col("DOLocationID") === zoneLookup("LocationID"),
        "left"
      )
      .select(
        col("DOLocationID"),
        col("Zone"),
        col("Borough"),
        col("count")
      )

    // =====================================================
    // 2. ACTIVITE TEMPORELLE
    // =====================================================
    val tripsByHour = df.groupBy("hour")
      .count()
      .orderBy("hour")

    val tripsByDay = df.groupBy("day_of_week")
      .count()
      .orderBy(desc("count"))

    // =====================================================
    // 3. HOTSPOTS SPATIO-TEMPORELS
    // =====================================================
    val hotspots = df
      .groupBy("hour", "PULocationID")
      .count()
      .orderBy(desc("count"))
      .limit(10)
      .join(
        zoneLookup,
        col("PULocationID") === zoneLookup("LocationID"),
        "left"
      )
      .select(
        col("hour"),
        col("PULocationID"),
        col("Zone"),
        col("Borough"),
        col("count")
      )

    // =====================================================
    // 4. FLUX ORIGINE → DESTINATION
    // =====================================================
    val odFlows = df
      .groupBy("PULocationID", "DOLocationID")
      .count()
      .orderBy(desc("count"))
      .limit(10)
      .join(
        zoneLookup
          .withColumnRenamed("Zone", "PU_Zone")
          .withColumnRenamed("Borough", "PU_Borough"),
        col("PULocationID") === col("LocationID"),
        "left"
      )
      .drop("LocationID")
      .join(
        zoneLookup
          .withColumnRenamed("Zone", "DO_Zone")
          .withColumnRenamed("Borough", "DO_Borough"),
        col("DOLocationID") === col("LocationID"),
        "left"
      )
      .drop("LocationID")

    // =====================================================
    // 5. DONNEES POUR HEATMAP (EXPORT CSV)
    // =====================================================
    val heatmapData = df
      .groupBy("PULocationID", "hour")
      .count()

    // =====================================================
    // 5. EXPORT DES RESULTATS
    // =====================================================
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()

    // 1. ECRITURE DU FICHIER TXT (RESULTATS)
    try {
      val writer = new PrintWriter(new File(outputDir, "phase3_results.txt"))
      
      writer.println("PHASE 3 – ANALYSE SPATIO-TEMPORELLE (BONUS)")
      writer.println("=========================================\n")

      writer.println("Top 10 zones de prise en charge (Pickup) :")
      topPickupZones.collect().foreach { r =>
        writer.println(s" - ${r.getAs[String]("Zone")} (${r.getAs[String]("Borough")}) : ${r.getAs[Long]("count")} trajets")
      }

      writer.println("\nTop 10 zones de depose (Dropoff) :")
      topDropoffZones.collect().foreach { r =>
        writer.println(s" - ${r.getAs[String]("Zone")} (${r.getAs[String]("Borough")}) : ${r.getAs[Long]("count")} trajets")
      }

      writer.println("\nNombre de trajets par heure :")
      tripsByHour.collect().foreach { r =>
        writer.println(s" - Heure ${r.getAs[Int]("hour")} : ${r.getAs[Long]("count")} trajets")
      }

      writer.println("\nNombre de trajets par jour de la semaine :")
      tripsByDay.collect().foreach { r =>
        writer.println(s" - ${r.getAs[String]("day_of_week")} : ${r.getAs[Long]("count")} trajets")
      }

      writer.println("\nTop 10 hotspots spatio-temporels :")
      hotspots.collect().foreach { r =>
        writer.println(s" - Heure ${r.getAs[Int]("hour")}, ${r.getAs[String]("Zone")} (${r.getAs[String]("Borough")}) : ${r.getAs[Long]("count")} trajets")
      }

      writer.println("\nTop 10 flux origine → destination :")
      odFlows.collect().foreach { r =>
        writer.println(s" - ${r.getAs[String]("PU_Zone")} → ${r.getAs[String]("DO_Zone")} : ${r.getAs[Long]("count")} trajets")
      }
      
      writer.flush()
      writer.close()
    } catch {
      case e: Exception => println(s"Erreur ecriture TXT: ${e.getMessage}")
    }

    // 2. ECRITURE DU CSV (HEATMAP)
    try {
      val heatmapManualFile = new File(outputDir, "heatmap_pickup_hour.csv")
      val localData = heatmapData.collect()
      val pw = new PrintWriter(heatmapManualFile)
      pw.println("PULocationID,hour,count")
      localData.foreach { r =>
        pw.println(s"${r.get(0)},${r.get(1)},${r.get(2)}")
      }
      pw.close()
    } catch {
      case e: Exception => println(s"Erreur ecriture CSV heatmap: ${e.getMessage}")
    }

    println("Phase 3 terminee – fichiers generes avec succes dans le dossier 'output/'")
    spark.stop()
  }
}
