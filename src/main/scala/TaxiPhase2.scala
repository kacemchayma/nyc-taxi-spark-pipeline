import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object TaxiPhase2 {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // Session Spark (Windows stable)
    // =====================================================
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Phase 2 Nettoyage & Transformations")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =====================================================
    // Lecture des données
    // =====================================================
    val inputPath =
      "data/yellow_parquet/yellow_tripdata_2025-03.parquet"

    val rawDF = spark.read.parquet(inputPath)

    // =====================================================
    // 1. NETTOYAGE DES DONNÉES
    // =====================================================
    val cleanDF = rawDF
      .withColumn("pickup_ts", col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", col("tpep_dropoff_datetime").cast("timestamp"))
      .filter(col("pickup_ts").isNotNull && col("dropoff_ts").isNotNull)
      .filter(col("trip_distance") > 0)
      .filter(col("fare_amount") >= 0)
      .filter(col("passenger_count") > 0)
      .filter(col("dropoff_ts") > col("pickup_ts"))

    // =====================================================
    // 2. COLONNES DÉRIVÉES
    // =====================================================
    val enrichedDF = cleanDF
      .withColumn(
        "trip_duration",
        (unix_timestamp(col("dropoff_ts")) -
          unix_timestamp(col("pickup_ts"))) / 60
      )
      .filter(col("trip_duration") > 0 && col("trip_duration") < 300)
      .withColumn(
        "average_speed",
        col("trip_distance") / (col("trip_duration") / 60)
      )
      .withColumn("hour", hour(col("pickup_ts")))
      .withColumn("day_of_week", date_format(col("pickup_ts"), "E"))
      .withColumn(
        "trip_category",
        when(col("trip_distance") < 10, "short_trip")
          .otherwise("long_trip")
      )

    // =====================================================
    // 3. ANALYSES
    // =====================================================
    val totalTrips = enrichedDF.count()

    val tripCategoryDist = enrichedDF
      .groupBy("trip_category")
      .count()

    val speedByHour = enrichedDF
      .groupBy("hour")
      .agg(round(avg("average_speed"), 2).alias("avg_speed_kmh"))
      .orderBy("hour")

    val speedByDay = enrichedDF
      .groupBy("day_of_week")
      .agg(round(avg("average_speed"), 2).alias("avg_speed_kmh"))

    val distanceStats =
      enrichedDF.select("trip_distance", "trip_duration").describe()

    // =====================================================
    // 4. ÉCRITURE DU FICHIER TEXTE
    // =====================================================
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdir()

    val writer = new PrintWriter("output/phase2_results.txt")

    writer.println("PHASE 2 – NETTOYAGE & TRANSFORMATIONS")
    writer.println("====================================\n")

    writer.println(s"Nombre de trajets après nettoyage : $totalTrips\n")

    writer.println("Distribution des distances et durées :")
    distanceStats.collect().foreach(r =>
      writer.println(r.mkString(" | "))
    )

    writer.println("\nProportion de trajets courts vs longs :")
    tripCategoryDist.collect().foreach(r =>
      writer.println(s" - ${r.getString(0)} : ${r.getLong(1)}")
    )

    writer.println("\nVitesse moyenne par heure :")
    speedByHour.collect().foreach(r =>
      writer.println(s" - Heure ${r.getInt(0)} : ${r.getDouble(1)} km/h")
    )

    writer.println("\nVitesse moyenne par jour de la semaine :")
    speedByDay.collect().foreach(r =>
      writer.println(s" - ${r.getString(0)} : ${r.getDouble(1)} km/h")
    )

    writer.close()

    println("Fichier output/phase2_results.txt généré avec succès ✅")

    spark.stop()
  }
}
