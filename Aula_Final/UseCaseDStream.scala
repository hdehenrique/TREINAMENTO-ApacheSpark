package br.com.scalasystems.curso.spark.usecase

import br.com.scalasystems.curso.spark.usecase.UseCaseJob.{partidos, processar}
import br.com.scalasystems.curso.spark.kafka._
import br.com.scalasystems.curso.spark.cassandra._
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.LocationStrategies

import scala.util.{Failure, Success}

object UseCaseDStream extends Serializable with LazyLogging{


  def main(args: Array[String]): Unit = {

    val kafka_topic = "spark_lab_votos"
    val kafka_broker_list = "localhost:9092"
    val kafka_groupid = "Spark_Lab_v1"
    val kafka_topic_npart = 1

    val sparkConf = new SparkConf()
    sparkConf
      .set("spark.cassandra.connection.host", "127.0.0.1")
      .set("spark.cassandra.connection.port", "9042")
      .set("spark.cassandra.input.consistency.level", "LOCAL_ONE")
      .set("spark.cassandra.output.consistency.level", "LOCAL_ONE")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.sql.streaming.metricsEnabled", "true")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.backpressure.pid.minRate", "10")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.kafka.isolation.level", "read_commited")
      .set("spark.streaming.backpressure.initialRate=", "10")
      .set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true")
      .set("spark.driver.cores", "3")
      .setAppName("UseCaseDStream")
      .setMaster("local")

    //https://kafka.apache.org/documentation/#max.partition.fetch.bytes
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    val context = StreamingContext.getActiveOrCreate(() =>
      functionToCreateContext(sparkContext, kafka_broker_list, kafka_topic, kafka_groupid, "1048576"))
    context.start()
    context.awaitTermination()
  }

  def functionToCreateContext(sparkContext:SparkContext, kafka_broker_list:String,
                              topic:String, groupId:String, maxPartitionFetchBytes:String): StreamingContext = {

    val preferredHosts = LocationStrategies.PreferConsistent
    val kafkaParams: Map[String, Object] =
      Map("bootstrap.servers" -> kafka_broker_list,
        "auto.offset.reset" -> "earliest",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "enable.auto.commit" -> Boolean.box(false),
        "max.partition.fetch.bytes" -> maxPartitionFetchBytes /*"102760448"*/ ,
        //"session.timeout.ms" -> "300000",
        "isolation.level" -> "read_committed",
        "group.id" -> groupId)

    val ssc = SparkStreamContext.getInstance(sparkContext)

    val partidosDominio = partidos(sparkContext)
    val partidosDominioRef = sparkContext.broadcast(partidosDominio)
    val cassandraOffsetStore:OffsetsStore = new CassandraOffsetStore()
    val dstream = KafkaSource.kafkaStream(ssc, kafkaParams, preferredHosts, cassandraOffsetStore, groupId , topic, 0)
    val results = dstream.map(record => (record.key(), record.value()))
    val values = results.map(elem => elem._2)
    values.foreachRDD{rdd =>
      //Nao precisamos mas seria aqui
      //rdd.cache()
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val json = spark.read.schema(UseCaseJob.schema()).option("mode", "FAILFAST").json(rdd)

      UseCaseJob.processar(spark, json, partidosDominioRef) match {
        case Success(_) => logger.info("UseCaseJob processou com sucesso")
        case Failure(exception) =>
          logger.info("Excecao ao executar UseCaseJob")
          throw exception
      }
      //rdd.unpersist()
    }
    ssc
  }
}

object SparkStreamContext extends Serializable {
  @transient  private var instance: StreamingContext = _
  def getInstance(sparkContext: SparkContext): StreamingContext = {
    if (instance == null) {
      instance = new StreamingContext(sparkContext, Seconds(5))
    }
    instance
  }
}

object SparkSessionSingleton extends Serializable{
  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
