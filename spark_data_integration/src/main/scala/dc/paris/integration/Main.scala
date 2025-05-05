package dc.paris.integration

import org.apache.spark.sql.SparkSession

//import java.io.File



object Main extends App {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Data Integration")
    .master("local[*]")
    .config("fs.s3a.access.key", "vPNJQMl0tM2TfedU582C") // A renseigner
    .config("fs.s3a.secret.key", "V2njsRt5phGd0sSpjwzWTT87Nz36KgmSrEiHVXLC") // A renseigner
    .config("fs.s3a.endpoint", "http://localhost:9000/")
    .config("fs.s3a.path.style.access", "true")
    .config("fs.s3a.connection.ssl.enable", "false")
    .config("fs.s3a.attempts.maximum", "1")
    .config("fs.s3a.connection.establish.timeout", "1000")
    .config("fs.s3a.connection.timeout", "5000")
    .getOrCreate()

  val localFiles = Seq(
    "../data/raw/yellow_tripdata_2024-10.parquet",
    "../data/raw/yellow_tripdata_2024-11.parquet",
    "../data/raw/yellow_tripdata_2024-12.parquet"
  )

  val bucketName = "yellow-taxi" // Remplace par ton bucket réel

  localFiles.foreach { filePath =>
    val fileName = filePath.split("/").last
    val destinationPath = s"s3a://$bucketName/$fileName"

    println(s"Uploading $filePath to $destinationPath")

    val df = spark.read.parquet(filePath)
    df.write.mode("overwrite").parquet(destinationPath)

    println(s"✔ Uploaded $filePath to $destinationPath")
  }

  spark.stop()
}
