import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.Pipeline
import java.io.{File, PrintWriter}

object TaxiPhase7_ML {

  def main(args: Array[String]): Unit = {

    // Utilisation des utilitaires centralisés
    val spark = TaxiUtils.getSparkSession("NYC Taxi - Phase 7 Machine Learning")
    spark.sparkContext.setLogLevel("ERROR")
    TaxiUtils.ensureOutputDir()

    println("Phase 7 : Chargement et préparation des données pour le ML...")

    // 1. LECTURE ET NETTOYAGE
    val rawDF = spark.read.parquet(TaxiUtils.DATA_PATH)
      .withColumn("pickup_ts", col("tpep_pickup_datetime").cast("timestamp"))
      .withColumn("hour", hour(col("pickup_ts")))
      .withColumn("day_of_week", date_format(col("pickup_ts"), "E"))
      .filter(col("total_amount") > 0 && col("total_amount") < 200)
      .filter(col("trip_distance") > 0 && col("trip_distance") < 50)
      .select("PULocationID", "DOLocationID", "trip_distance", "hour", "day_of_week", "total_amount")
      .na.drop()

    // 2. FEATURE ENGINEERING (Pipeline)
    val dayIndexer = new StringIndexer()
      .setInputCol("day_of_week")
      .setOutputCol("day_index")

    val assembler = new VectorAssembler()
      .setInputCols(Array("PULocationID", "DOLocationID", "trip_distance", "hour", "day_index"))
      .setOutputCol("features")

    // 3. SPLIT TRAIN/TEST
    val Array(trainingData, testData) = rawDF.randomSplit(Array(0.8, 0.2), seed = 1234)

    // 4. ENTRENEMENT DU MODELE (Random Forest)
    println("Entraînement du modèle Random Forest...")
    val rf = new RandomForestRegressor()
      .setLabelCol("total_amount")
      .setFeaturesCol("features")
      .setNumTrees(20)

    val pipeline = new Pipeline().setStages(Array(dayIndexer, assembler, rf))
    val model = pipeline.fit(trainingData)

    // 5. PREDICTIONS ET EVALUATION
    println("Évaluation du modèle...")
    val predictions = model.transform(testData)

    val evaluator_rmse = new RegressionEvaluator()
      .setLabelCol("total_amount")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val evaluator_r2 = new RegressionEvaluator()
      .setLabelCol("total_amount")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val rmse = evaluator_rmse.evaluate(predictions)
    val r2 = evaluator_r2.evaluate(predictions)

    println(s"RMSE: $rmse")
    println(s"R2: $r2")

    // 6. SAUVEGARDE DES RESULTATS
    val writer = new PrintWriter(new File(TaxiUtils.OUTPUT_DIR, "phase7_ml_results.txt"))
    try {
      writer.println("PHASE 7 – PREDICTION DES TARIFS (MACHINE LEARNING)")
      writer.println("================================================\n")
      writer.println(s"Modèle utilisé : Random Forest Regressor")
      writer.println(s"Nombre de trajets pour l'entraînement : ${trainingData.count()}")
      writer.println(s"Nombre de trajets pour le test : ${testData.count()}\n")
      
      writer.println("Evaluation du modèle :")
      writer.println(s" - RMSE (Root Mean Squared Error) : ${"%.2f".format(rmse)}")
      writer.println(s" - R2 (Coefficient de détermination) : ${"%.2f".format(r2)}\n")
      
      writer.println("Echantillon de prédictions :")
      predictions.select("trip_distance", "total_amount", "prediction")
        .limit(10)
        .collect()
        .foreach { r =>
          val distance = r.get(0)
          val reel = r.get(1)
          val predit = r.getDouble(2)
          writer.println(s" - Distance: $distance km | Réel: $reel$$ | Prédit: ${"%.2f".format(predit)}$$")
        }
    } finally {
      writer.close()
    }

    println("✅ Phase 7 terminée – fichier output/phase7_ml_results.txt généré")
    spark.stop()
  }
}
