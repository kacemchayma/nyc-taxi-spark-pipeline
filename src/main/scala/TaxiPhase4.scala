import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}

object TaxiPhase4 {

  def main(args: Array[String]): Unit = {

    // =====================================================
    // CONFIG WINDOWS / SPARK
    // =====================================================
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Phase 4 Paiements & Pourboires (Final)")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // =====================================================
    // LECTURE DES DONNÉES + NETTOYAGE MINIMUM
    // =====================================================
    val taxiPath =
      "data/yellow_parquet/yellow_tripdata_2025-03.parquet"

    val df = spark.read
      .parquet(taxiPath)
      .withColumn("pickup_ts", col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("dropoff_ts", col("tpep_dropoff_datetime").cast("timestamp"))
      .filter(col("pickup_ts").isNotNull && col("dropoff_ts").isNotNull)
      .filter(col("fare_amount") > 0)
      .filter(col("total_amount") >= 0)
      .filter(col("trip_distance") > 0)
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

    // =====================================================
    // 1. DISTRIBUTION DES MODES DE PAIEMENT
    // =====================================================
    val paymentDistribution = df
      .groupBy("payment_type")
      .count()
      .orderBy(desc("count"))

    // =====================================================
    // 2. ANALYSE DES POURBOIRES
    // =====================================================
    val tipStats = df
      .withColumn("tip_ratio", col("tip_amount") / col("fare_amount"))
      .groupBy("payment_type")
      .agg(
        round(avg("tip_amount"), 2).alias("avg_tip"),
        round(avg("tip_ratio") * 100, 2).alias("avg_tip_percent"),
        round(sum("tip_amount"), 2).alias("total_tips")
      )

    // =====================================================
    // 3. POURBOIRES SELON LE TEMPS
    // =====================================================
    val tipsByHour = df
      .groupBy("hour")
      .agg(round(avg("tip_amount"), 2).alias("avg_tip"))
      .orderBy("hour")

    val tipsByDay = df
      .groupBy("day_of_week")
      .agg(round(avg("tip_amount"), 2).alias("avg_tip"))
      .orderBy(desc("avg_tip"))

    // =====================================================
    // 4. COMPARAISON PAR MODE DE PAIEMENT
    // =====================================================
    val paymentComparison = df
      .groupBy("payment_type")
      .agg(
        round(avg("total_amount"), 2).alias("avg_total_amount"),
        round(avg("trip_distance"), 2).alias("avg_distance"),
        round(avg("average_speed"), 2).alias("avg_speed")
      )

    // =====================================================
    // 5. ÉCRITURE DU FICHIER TXT (ROBUSTE)
    // =====================================================
    val outputDir = new File("output")
    if (!outputDir.exists()) outputDir.mkdirs()

    val writer = new PrintWriter(new File(outputDir, "phase4_results.txt"))

    try {

      writer.println("PHASE 4 – ANALYSE DES PAIEMENTS & POURBOIRES")
      writer.println("===========================================\n")

      writer.println("Distribution des modes de paiement :")
      paymentDistribution.collect().foreach { r =>
        writer.println(
          s" - payment_type ${r.getAs[Int]("payment_type")} : ${r.getAs[Long]("count")} trajets"
        )
      }

      writer.println("\nStatistiques des pourboires par mode de paiement :")
      tipStats.collect().foreach { r =>
        writer.println(
          s" - payment_type ${r.getAs[Int]("payment_type")} : " +
            s"pourboire moyen = ${r.getAs[Double]("avg_tip")}, " +
            s"pourcentage moyen = ${r.getAs[Double]("avg_tip_percent")} %, " +
            s"total pourboires = ${r.getAs[Double]("total_tips")}"
        )
      }

      writer.println("\nPourboire moyen selon l’heure :")
      tipsByHour.collect().foreach { r =>
        writer.println(
          s" - Heure ${r.getAs[Int]("hour")} : ${r.getAs[Double]("avg_tip")}"
        )
      }

      writer.println("\nPourboire moyen selon le jour de la semaine :")
      tipsByDay.collect().foreach { r =>
        writer.println(
          s" - ${r.getAs[String]("day_of_week")} : ${r.getAs[Double]("avg_tip")}"
        )
      }

      writer.println("\nComparaison montants / distances / vitesses par paiement :")
      paymentComparison.collect().foreach { r =>
        writer.println(
          s" - payment_type ${r.getAs[Int]("payment_type")} : " +
            s"montant moyen = ${r.getAs[Double]("avg_total_amount")}, " +
            s"distance moyenne = ${r.getAs[Double]("avg_distance")}, " +
            s"vitesse moyenne = ${r.getAs[Double]("avg_speed")} km/h"
        )
      }

    } finally {
      writer.close()
    }

    println("✅ Phase 4 terminée – fichier output/phase4_results.txt généré")

    spark.stop()
  }
}
