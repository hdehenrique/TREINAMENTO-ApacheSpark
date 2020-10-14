package br.com.scalasystems.curso.spark.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait OffsetsStore {

  def readOffsets(groupId: String, spark:SparkSession): Option[Map[TopicPartition, Long]]

  def saveOffsets(groupId: String, topic:String, spark:SparkSession, rdd: RDD[_]): Unit

}
