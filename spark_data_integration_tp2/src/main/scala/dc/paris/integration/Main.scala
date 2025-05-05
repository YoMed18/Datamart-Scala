package dc.paris.integration

import org.apache.spark.sql.{SaveMode, SparkSession}

// import java.io.File

object Main extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Parquet to PostgreSQL")
    .master("local[*]")
    .config("fs.s3a.access.key", "vPNJQMl0tM2TfedU582C")
    .config("fs.s3a.secret.key", "V2njsRt5phGd0sSpjwzWTT87Nz36KgmSrEiHVXLC")
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .getOrCreate()

  // Chemin vers le fichier parquet dans Minio
  val parquetPath = "s3a://yellow-taxi/yellow_tripdata_2024-10.parquet"

  // Lecture du fichier parquet
  val df = spark.read.parquet(parquetPath)

  // Paramètres PostgreSQL
  val jdbcUrl = "jdbc:postgresql://localhost:15432/datawarehouse"
  val dbTable = "yellow_taxi_trips_2024_10"
  val dbUser = "postgres"
  val dbPassword = "admin"

  // Écriture dans PostgreSQL
  df.write
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", dbTable)
    .option("user", dbUser)
    .option("password", dbPassword)
    .option("driver", "org.postgresql.Driver")
    .mode(SaveMode.Overwrite) // ou "append"
    .save()

  println(s"✔ Données écrites dans la table $dbTable")

  spark.stop()
}
