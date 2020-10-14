package br.com.scalasystems.curso.spark.cassandra

import br.com.scalasystems.curso.spark.kafka.{OffsetsStore, Stopwatch}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import com.typesafe.scalalogging.LazyLogging

import com.datastax.spark.connector._
import com.datastax.spark.connector.UDTValue.UDTValueConverter

import scala.util.{Failure, Success, Try}


case class tpic_offs(tpic_part:Integer, tpic_foff:Long, tpic_uoff:Long)
case class kafk_tpic_offs(nom_grpo:String, nom_tpic:String, tpic_offs:Set[tpic_offs])

class CassandraOffsetStore extends Serializable with OffsetsStore with LazyLogging {

  override def readOffsets(groupId: String, spark:SparkSession): Option[Map[TopicPartition, Long]] = {
    logger.info("Reading offsets from Cassandra")
    val stopwatch = new Stopwatch()

    val results = spark.sparkContext.cassandraTable("spark_lab", "kafk_tpic_offs")
      .select("nom_grpo", "nom_tpic", "tpic_offs")
      .where("nom_grpo = ?", groupId)
      .map { cRow =>
        val nom_tpic: String = cRow.getString("nom_tpic")
        val tpic_offs_udtvalue = cRow.getSet[UDTValue]("tpic_offs")

        val tpic_offs_map = tpic_offs_udtvalue.flatMap { udtvalue =>
          val tpic_part = udtvalue.getInt("tpic_part")
          val tpic_foff = udtvalue.getLong("tpic_foff")
          Map(tpic_part -> tpic_foff)
        }
        //Agrupando para a mesma particao pois esta vindo dois offsets diferentes para a mesma particao
        //o   que vai resultar do group (0, Set[(0, 216), (0, 512)]
        val tpic_offs_types_aggrp = tpic_offs_map.groupBy(a => a._1)
        //Dessa forma nos interessa comecar a partir do menor offset, nesse caso, ressaltamos que o processo
        //deve ser imdepotente pois pode ser que seja processado linhas novamente
        //Set[(0,216)]
        val tpic_offs_types_min = tpic_offs_types_aggrp.mapValues(mvalues => mvalues.minBy(tvalue => tvalue._2))
        val tpic_offs_types = tpic_offs_types_min.map { mvalues =>
          val (tpic_part, tpic_foff) = mvalues._2
          val topicPartition = new TopicPartition(nom_tpic, tpic_part)
          (topicPartition -> tpic_foff)
        }
        tpic_offs_types
      }
      Try( results.first() ) match {
        case Success(results) => {
          logger.info("Done reading offsets from Cassandra. Took " + stopwatch)
          Some(results)
        }
        case Failure(_) => None /*O first quando aplicado a uma colecao vazia retorna um IllegalArgumentException*/

      }
  }

  override def saveOffsets(groupId:String, topic: String, spark: SparkSession, rdd: RDD[_]): Unit = {

    logger.info("Saving offsets to Cassandra")
    val stopwatch = new Stopwatch()
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    offsetsRanges.foreach(offsetRange => logger.info(s"Using ${offsetRange}"))

    val topic_foffs = offsetsRanges.map { offsetRange =>
      val partition = offsetRange.partition
      val fromOffset = offsetRange.fromOffset
      val untilOffset = offsetRange.untilOffset
      tpic_offs(partition, fromOffset, untilOffset)
    }
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    logger.info(s"Writing offsets to Cassandra: ${offsetsRangesStr}")
    val kafka: Array[kafk_tpic_offs] = (kafk_tpic_offs(groupId, topic, topic_foffs.toSet) :: Nil).toArray
    val ds = spark.sparkContext.parallelize[kafk_tpic_offs](kafka)
    ds.saveToCassandra("spark_lab", "kafk_tpic_offs",
      SomeColumns("nom_grpo", "nom_tpic", "tpic_offs" overwrite))
    logger.info("Done updating offsets in Cassandra. Took " + stopwatch)
  }

}
