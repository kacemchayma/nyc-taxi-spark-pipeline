import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object TaxiPhase1 {

  def main(args: Array[String]): Unit = {

    // FIX Hadoop Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Phase 1 Ingestion & Exploration")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.hadoopConfiguration.set(
      "fs.file.impl",
      "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    spark.sparkContext.hadoopConfiguration.set(
      "io.native.lib.available",
      "false"
    )

    val taxiDF = spark.read
      .parquet("data/yellow_parquet/yellow_tripdata_2025-03.parquet")

    // =====================================================
    // CALCULS (AVANT ÉCRITURE)
    // =====================================================
    val totalTrips = taxiDF.count()

    val period = taxiDF.select(
      min("tpep_pickup_datetime").alias("start"),
      max("tpep_pickup_datetime").alias("end")
    ).first()

    val nullCounts = taxiDF.select(
      taxiDF.columns.map(c =>
        sum(col(c).isNull.cast("int")).alias(c)
      ): _*
    ).first()

    val invalidDistance = taxiDF.filter(col("trip_distance") <= 0).count()
    val negativeFare = taxiDF.filter(col("fare_amount") < 0).count()
    val invalidPassengers = taxiDF.filter(col("passenger_count") <= 0).count()
    val invalidDates =
      taxiDF.filter(col("tpep_dropoff_datetime") <= col("tpep_pickup_datetime")).count()

    // =====================================================
    // CRÉATION DU DOSSIER output
    // =====================================================
    val outputDir = new File("output")
    if (!outputDir.exists()) {
      outputDir.mkdir()
    }

    // =====================================================
    // ÉCRITURE DU FICHIER TXT
    // =====================================================
    val writer = new PrintWriter(new File("output/phase1_results.txt"))

    writer.println("PHASE 1 – INGESTION ET EXPLORATION INITIALE")
    writer.println("==========================================\n")

    writer.println(s"Nombre total de trajets : $totalTrips\n")

    writer.println("Période couverte :")
    writer.println(s" - Début : ${period.get(0)}")
    writer.println(s" - Fin   : ${period.get(1)}\n")

    writer.println("Valeurs NULL par colonne :")
    taxiDF.columns.zipWithIndex.foreach { case (name, idx) =>
      writer.println(s" - $name : ${nullCounts.getLong(idx)}")
    }
    writer.println()

    writer.println("Valeurs aberrantes détectées :")
    writer.println(s" - Distance <= 0 : $invalidDistance")
    writer.println(s" - Tarif négatif : $negativeFare")
    writer.println(s" - Passagers <= 0 : $invalidPassengers")
    writer.println(s" - Dates incohérentes : $invalidDates")

    writer.close()

    println("Fichier output/phase1_results.txt généré avec succès ✅")

    spark.stop()
  }
}
