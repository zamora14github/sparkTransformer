import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object KafkaToSpark {

  import org.apache.spark.sql.SparkSession

  /** Configures and executes the streaming context
   *
   * Create the Spark session/s, reads from Kafka with the specified options,
   * infer the schema from a file and starts the streaming job/s.
   *
   */
  def main(args: Array[String]): Unit = {

    // Creates Spark Session
    val spark = SparkSession
      .builder()
      .appName("KafkaSparkDemo")
      .config("spark.master", "local") // Delete when working in Docker
      .getOrCreate()



    val ds1 = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "10.0.0.3:9092,10.0.0.7:9093,10.0.0.8:9094")
      .option("subscribe", "modbusPLC0,s7PLC0,s7PLC1,s7PLC2,s7PLC3,s7PLC4,s7PLC5,s7PLC6,s7PLC7,s7PLC8,s7PLC9,s7PLC10" +
        ",s7PLC11,s7PLC12,s7PLC13,s7PLC14,s7PLC15,s7PLC16,s7PLC17,s7PLC18,s7PLC19,s7PLC20,s7PLC21,s7PLC22,s7PLC23" +
        ",s7PLC0,s7PLC24,s7PLC25,s7PLC26,s7PLC27,s7PLC28,s7PLC29,s7PLC30,s7PLC31,s7PLC32,s7PLC33,s7PLC34,s7PLC35" +
        ",s7PLC36,s7PLC37,plcAlarms")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss" , "false")
      //.option("multiLine", true)
      .load().withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
      .withColumn("value", regexp_replace(col("value"), "^\"|\"$", ""))


    print("Printing ds1 schema (coming from Kafka))");
    ds1.printSchema()


    // Infer the schema from a file
    val fileStream = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/batchNameParsed.txt"))
    val contents =  fileStream.getLines().mkString("\n")

    import spark.implicits._

    val schemaInferred = spark.read.json(Seq(contents).toDS).schema

    print("This is the schema inferred coming from Kafka: ")
    schemaInferred.printTreeString()


    val eventWithSchema = ds1.selectExpr("CAST(value as STRING) as json")
      .select(from_json($"json", schema = schemaInferred).as("data")).select("data.*")



    // Select the available values
    val extracted = eventWithSchema.select("address","model","sourceName","timestamp","values.Q0","values.Q1",
      "values.Q2","values.Q3","values.I0","values.I1","values.I2","values.I3","values.coil1","values.holdingRegister1",
      "alarm_message","alarm_timestamp","alarm_address")


    val url = "jdbc:mysql://10.0.0.2:3306/plc4x"
    val user = "root"
    val pwd = "root"

    val writer = new JDBCSink(url,user,pwd)

    val startQueries = extracted.writeStream//.option("checkpointLocation", "/build/checkpoint")
        .trigger(Trigger.ProcessingTime("1 seconds")).foreach(writer).start()
    startQueries.awaitTermination()

  }

}