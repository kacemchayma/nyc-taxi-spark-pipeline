# Copilot Instructions for Spark Taxi Analysis

## Project Overview
A Scala/Apache Spark application for analyzing NYC taxi trip data. The project uses sbt as the build tool and runs Spark in local mode (`master("local[*]")`).

## Architecture
- **Main Application**: `TaxiAnalysis.scala` - Single entry point using Spark Session
- **Data Sources**: 
  - `data/yellow_taxi.csv` - Trip data (loaded with inferred schema)
  - `data/taxi_zone_lookup.csv` - Reference data for zone mapping
- **Execution Model**: Spark runs in local multi-threaded mode for development/testing

## Key Patterns & Conventions

### Spark Configuration
- Always use `SparkSession.builder()` for session creation
- Enable `local[*]` mode for local development (uses all available cores)
- Set log level to WARN to reduce output clutter: `spark.sparkContext.setLogLevel("WARN")`
- Always call `spark.stop()` at the end to release resources

### Data Loading
- CSV files use header option and schema inference enabled
- Path references are relative to project root (e.g., `data/yellow_taxi.csv`)
- When adding new data sources, follow the same CSV loading pattern

### Code Style
- Use object with main method as application entry point
- Import Spark functions explicitly (`import org.apache.spark.sql.functions._`)
- Output analysis results to stdout with descriptive headers (e.g., `println("=== Schéma des données ===")`)
- French comments are used in this project - maintain this style for consistency

## Build & Execution

### Commands
- **Run**: `sbt run` - Executes the main application
- **Compile**: `sbt compile` - Builds the project
- **Clean**: `sbt clean` - Removes build artifacts

### Build Configuration
**CRITICAL**: A `build.sbt` file is required in the project root. The current setup is missing this configuration.

Required `build.sbt` structure:
```scala
name := "spark-taxi-analysis"
version := "1.0"
scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.spark" %% "spark-core" % "3.5.0"
)

mainClass in (Compile, run) := Some("TaxiAnalysis")
```

- sbt version: 1.10.7
- Scala source files in project root
- All dependencies resolved through libraryDependencies in build.sbt

## Development Workflow

### Adding Analytics
1. Load CSV data using existing pattern with `.option("header", "true").option("inferSchema", "true")`
2. Use Spark SQL functions from `org.apache.spark.sql.functions._`
3. Validate results with `.show()` and `.count()`
4. Output findings with labeled print statements

### Common Spark Operations (Already Established)
- `printSchema()` - Display column structure
- `show(n)` - Display first n rows
- `count()` - Get total row count
- Use `s"$variable"` string interpolation for output

## Troubleshooting
- **Missing build.sbt**: If sbt fails with "Neither build.sbt nor a 'project' directory", create `build.sbt` in project root (see Build Configuration section above)
- If `sbt run` exits with code 1 after build.sbt exists, check that data files exist in `data/` directory with exact filenames
- CSV file paths are case-sensitive on Unix-like systems
- Ensure Spark/Scala libraries are properly declared in build.sbt libraryDependencies
- Large dataset processing may require adjusting local[*] to local[n] with specific core count

## Integration Points
- No external APIs or services currently integrated
- Data is purely file-based from CSV sources
- All analysis output goes to stdout (future versions may add file output)

