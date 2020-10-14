package br.com.scalasystems.curso.spark.kafka


import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag


object KafkaSource extends Serializable with LazyLogging {

  def kafkaStream[K: ClassTag, V: ClassTag]
  (ssc: StreamingContext, kafkaParams: Map[String, Object], locationStrategy: LocationStrategy,
   offsetsStore: OffsetsStore, groupId:String, topic: String, numPart:Int): InputDStream[ConsumerRecord[String, String]] = {

    val sparkSession = SparkSession.builder.config(ssc.sparkContext.getConf).getOrCreate()
    val topics = Set(topic)

    val storedOffsets = offsetsStore.readOffsets(groupId, sparkSession)
    val kafkaStream:InputDStream[ConsumerRecord[String, String]]  = storedOffsets match {
      case None =>
        // start from the latest offsets
        org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
        //val partitions = List.tabulate(numPart)(n => new TopicPartition(topic, n))
        //org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, ConsumerStrategies.Assign[String, String](partitions, kafkaParams))
      case Some(fromOffsets) =>
        // start from previously saved offsets
        org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, fromOffsets))
        //org.apache.spark.streaming.kafka010.KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
    }

    kafkaStream.foreachRDD{
      rdd => offsetsStore.saveOffsets(groupId, topic, sparkSession,rdd)
      //val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetsRanges)
    }

    kafkaStream
  }



}

