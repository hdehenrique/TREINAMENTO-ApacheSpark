package br.com.scalasystems.curso.spark.usecase

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import com.datastax.spark.connector._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.cassandra._

import scala.util.{Failure, Success, Try}

/**
 *
 *
 * {"cpf":"26890918878", "genero": "F", "idade": 33, "sigla": "SCA"}
 *
 *  //1 - Criar o structType
 *  //2 - Criar o main carregando o arquivo json do disco para validar o schema :)
 *
 */

//MSG Kafka
case class Voto(cpf:String, genero:String, idade:Int, sigla:String)

// Tabelas Cassandra
case class Partido(sigla:String, partido:String)
case class Votos(cpf:String, genero:String, idade:Int, partido:String)

// dados trafegados pela rede tem que ser Serializable, também foi add log LazyLogging
object UseCaseJob extends Serializable with LazyLogging {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("UseCaseJob")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .config("spark.cassandra.connection.port", "9042")
      .config("sparsk.cassandra.input.consistency.level", "LOCAL_ONE")
      .getOrCreate()

// leitura do arquivo json(local)
    val json = spark.read.schema(schema())
      .option("multiline", "true")
      .option("mode", "FAILFAST")
      .json(spark.sparkContext.wholeTextFiles("/usr/local/dados/temp/votos.json").values)
    //json.show(truncate = false)

   
     //Criar o broadcast do partido
     val partidosDominio = partidos(spark.sparkContext)
     val partidosDominioRef = spark.sparkContext.broadcast(partidosDominio)
     processar(spark, json, partidosDominioRef)
  }

  def processar(spark:SparkSession, json:DataFrame, partidosDominioRef:Broadcast[Map[String, String]]) :Try[Unit] = {
      try {
        json.map { row =>
          val sigla = row.getAs[String]("sigla")
          val partido: Option[String] = partidosDominioRef.value.get(sigla)
          logger.debug(s"Processando os dados : sigla [${sigla}]")
          Votos(
            row.getAs[String]("cpf"),
            row.getAs[String]("genero"),
            row.getAs[Int]("idade"),
            partido.get
          )
        }(Encoders.product[Votos])
          .write
          .format("org.apache.spark.sql.cassandra")
          .options(
            Map("keyspace" -> "spark_lab", "table" -> "voters")
          ).mode(SaveMode.Append).save()
      }catch {
        case e: Throwable =>
          logger.error(s"UseCaseJob error")
          throw e
      }
      Success(Nil)
  }

  def partidos(sparkContext:SparkContext) :Map[String, String] = {
      sparkContext.cassandraTable("spark_lab", "partidos")
        .select("sigla", "partido")
        .flatMap{cRow =>
          Map(cRow.getString("sigla") -> cRow.getString("partido"))
        }.collect().toMap
  }

  //Definir o schema da estrutura de leitura do KAFKA
  //{"cpf":"26890918878", "genero": "F", "idade": 33, "sigla": "SCA"}
  def schema(): StructType = {
    StructType(
      Array(
      StructField("cpf", StringType),
      StructField("genero", StringType),
      StructField("idade", IntegerType),
      StructField("sigla", StringType)
      )
    )
  }

}
