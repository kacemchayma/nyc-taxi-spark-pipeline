import org.apache.spark.sql.SparkSession
import java.io.File

object TaxiUtils {

  // Configuration Hadoop pour Windows
  def setupHadoop(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
  }

  // Création de la Session Spark centralisée
  def getSparkSession(appName: String): SparkSession = {
    setupHadoop()
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }

  // Chemins des données
  val DATA_PATH = "data/yellow_parquet/yellow_tripdata_2025-03.parquet"
  val ZONE_LOOKUP_PATH = "data/taxi_zone_lookup.csv"
  val OUTPUT_DIR = "output"

  // S'assurer que le dossier output existe
  def ensureOutputDir(): Unit = {
    val dir = new File(OUTPUT_DIR)
    if (!dir.exists()) dir.mkdirs()
  }
}
