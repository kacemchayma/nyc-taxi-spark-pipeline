import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object TaxiPhase5 {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // CONFIG WINDOWS / SPARK
    // =====================================================
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Phase 5 Ride-Sharing")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =====================================================
    // LECTURE ET PREPARATION DES DONNEES
    // =====================================================
    val taxiPath =
      "data/yellow_parquet/yellow_tripdata_2025-03.parquet"

    val df = spark.read.parquet(taxiPath)
      .withColumn("pickup_ts", col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", col("tpep_dropoff_datetime").cast("timestamp"))
      .filter(col("pickup_ts").isNotNull && col("dropoff_ts").isNotNull)
      .filter(col("trip_distance") > 0 && col("fare_amount") > 0)
      .withColumn(
        "trip_duration",
        (unix_timestamp(col("dropoff_ts")) -
          unix_timestamp(col("pickup_ts"))) / 60
      )
      .filter(col("trip_duration") > 0 && col("trip_duration") < 300)

    // =====================================================
    // 1. SELECTION DES TRAJETS COURTS
    // =====================================================
    val shortTrips = df.filter(col("trip_distance") < 5)
    val totalShortTrips = shortTrips.count()

    // =====================================================
    // 2. FENETRAGE TEMPOREL (10 MINUTES)
    // =====================================================
    val shortTripsWindowed = shortTrips
      .withColumn("time_window", window(col("pickup_ts"), "10 minutes"))

    // =====================================================
    // 3. GROUPES COVOITURABLES
    // =====================================================
    val rideSharingGroups = shortTripsWindowed
      .groupBy("PULocationID", "time_window")
      .agg(
        count("*").alias("trips_in_group"),
        sum("trip_distance").alias("total_distance"),
        sum("fare_amount").alias("total_fare"),
        sum("trip_duration").alias("total_duration")
      )
      .filter(col("trips_in_group") >= 2)

    val totalRideSharingGroups = rideSharingGroups.count()

    // =====================================================
    // 4. ESTIMATION DES GAINS
    // =====================================================
    val gains = rideSharingGroups
      .withColumn("distance_saved", col("total_distance") * 0.20)
      .withColumn("fare_saved", col("total_fare") * 0.15)
      .withColumn("time_saved", col("total_duration") * 0.10)

    val globalGains = gains.agg(
      round(sum("distance_saved"), 2).alias("distance_saved"),
      round(sum("fare_saved"), 2).alias("fare_saved"),
      round(sum("time_saved"), 2).alias("time_saved")
    ).collect()(0)

    // =====================================================
    // 5. ECRITURE DU FICHIER TXT
    // =====================================================
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()

    val writer = new PrintWriter(new File(outputDir, "phase5_results.txt"))

    try {
      writer.println("PHASE 5 – EXPLORATION DU RIDE-SHARING")
      writer.println("===================================\n")

      writer.println(s"Nombre total de trajets courts (< 5 km) : $totalShortTrips")
      writer.println(s"Nombre de groupes covoiturables : $totalRideSharingGroups\n")

      writer.println("Hypotheses de gains appliquees :")
      writer.println(" - Reduction distance : 20 %")
      writer.println(" - Reduction cout     : 15 %")
      writer.println(" - Reduction temps    : 10 %\n")

      writer.println("Gains potentiels estimes :")
      writer.println(s" - Distance economisee : ${globalGains.getAs[Double]("distance_saved")} km")
      writer.println(s" - Cout economise      : ${globalGains.getAs[Double]("fare_saved")} dollars")
      writer.println(s" - Temps economise     : ${globalGains.getAs[Double]("time_saved")} minutes\n")

      writer.println("Conclusion :")
      writer.println(
        "L analyse montre que de nombreux trajets courts, proches dans le temps et l espace, " +
        "peuvent etre regroupes pour du covoiturage. Cette approche permettrait de reduire " +
        "les couts, les distances parcourues et les temps de trajet."
      )

    } finally {
      writer.close()
    }

    println("Phase 5 terminee – fichier output/phase5_results.txt genere")

    spark.stop()
  }
}
