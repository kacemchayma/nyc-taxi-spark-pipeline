import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object TaxiPhase6_Extension {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // CONFIG WINDOWS / SPARK
    // =====================================================
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Extension Analyse Avancee & Feature Engineering")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =====================================================
    // LECTURE & PREPARATION DES DONNEES
    // =====================================================
    val taxiPath =
      "data/yellow_parquet/yellow_tripdata_2025-03.parquet"

    val df = spark.read.parquet(taxiPath)
      .withColumn("pickup_ts", col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", col("tpep_dropoff_datetime").cast("timestamp"))
      .filter(col("pickup_ts").isNotNull && col("dropoff_ts").isNotNull)
      .filter(col("trip_distance") > 0 && col("fare_amount") >= 0)
      .withColumn(
        "trip_duration",
        (unix_timestamp(col("dropoff_ts")) -
          unix_timestamp(col("pickup_ts"))) / 60
      )
      .filter(col("trip_duration") > 0 && col("trip_duration") < 300)

    // =====================================================
    // FEATURE ENGINEERING
    // =====================================================
    val enrichedDF = df
      .withColumn("average_speed",
        col("trip_distance") / (col("trip_duration") / 60)
      )
      .withColumn("tip_percentage",
        when(col("fare_amount") > 0,
          col("tip_amount") / col("fare_amount") * 100
        ).otherwise(0)
      )
      .withColumn("hour", hour(col("pickup_ts")))
      .withColumn("day_of_week", date_format(col("pickup_ts"), "E"))
      .withColumn(
        "is_peak_hour",
        when(col("hour").between(7, 9) || col("hour").between(16, 19), 1).otherwise(0)
      )
      .withColumn(
        "trip_category",
        when(col("trip_distance") < 2, "short")
          .when(col("trip_distance") < 10, "medium")
          .otherwise("long")
      )

    val totalTrips = enrichedDF.count()

    // =====================================================
    // 1. TENDANCES TEMPORELLES
    // =====================================================
    val tripsByHour = enrichedDF.groupBy("hour")
      .count()
      .orderBy("hour")

    val revenueByHour = enrichedDF.groupBy("hour")
      .agg(round(sum("total_amount"), 2).alias("revenue"))
      .orderBy("hour")

    // =====================================================
    // 2. DETECTION DES ANOMALIES
    // =====================================================
    val anomalyShort = enrichedDF.filter(col("trip_distance") <= 0.5).count()
    val anomalyLong = enrichedDF.filter(col("trip_distance") >= 50).count()
    val anomalySpeed = enrichedDF.filter(col("average_speed") > 120).count()
    val anomalyDuration = enrichedDF.filter(col("trip_duration") > 180).count()

    // =====================================================
    // 3. FREQUENCE DES TRAJETS PAR ZONE
    // =====================================================
    val zoneFrequency = enrichedDF.groupBy("PULocationID")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    // =====================================================
    // 4. ECRITURE DU FICHIER TXT
    // =====================================================
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()

    val writer = new PrintWriter(new File(outputDir, "phase6_extension_results.txt"))

    try {
      writer.println("PHASE 6 – EXTENSION ANALYSE AVANCEE & FEATURE ENGINEERING")
      writer.println("=====================================================\n")

      writer.println(s"Nombre total de trajets analyses : $totalTrips\n")

      writer.println("Tendances horaires (nombre de trajets) :")
      tripsByHour.collect().foreach { r =>
        writer.println(
          s" - Heure ${r.getAs[Int]("hour")} : ${r.getAs[Long]("count")} trajets"
        )
      }

      writer.println("\nRevenus par heure :")
      revenueByHour.collect().foreach { r =>
        writer.println(
          s" - Heure ${r.getAs[Int]("hour")} : ${r.getAs[Double]("revenue")} dollars"
        )
      }

      writer.println("\nDetection des anomalies :")
      writer.println(s" - Trajets tres courts (<= 0.5 km) : $anomalyShort")
      writer.println(s" - Trajets tres longs (>= 50 km)   : $anomalyLong")
      writer.println(s" - Vitesses irreelles (> 120 km/h): $anomalySpeed")
      writer.println(s" - Durees excessives (> 180 min)  : $anomalyDuration")

      writer.println("\nTop 10 zones par frequence de trajets :")
      zoneFrequency.collect().foreach { r =>
        writer.println(
          s" - Zone ${r.getAs[Int]("PULocationID")} : ${r.getAs[Long]("count")} trajets"
        )
      }

      writer.println("\nFeatures creees pour ML :")
      writer.println(" - average_speed")
      writer.println(" - tip_percentage")
      writer.println(" - is_peak_hour")
      writer.println(" - trip_category")
      writer.println(" - zone_trip_frequency")

      writer.println("\nConclusion :")
      writer.println(
        "Cette extension enrichit les donnees taxis par des analyses avancees, " +
        "la detection d anomalies et la creation de variables explicatives. " +
        "Ces transformations permettent de mieux comprendre la dynamique des trajets " +
        "et preparent efficacement le jeu de donnees a des usages predictifs futurs."
      )

    } finally {
      writer.close()
    }

    println("Extension analyse avancee terminee – fichier phase6_extension_results.txt genere")

    spark.stop()
  }
}
