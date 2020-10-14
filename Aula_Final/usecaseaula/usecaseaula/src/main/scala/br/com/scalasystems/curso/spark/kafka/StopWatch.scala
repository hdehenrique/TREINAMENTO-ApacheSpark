package br.com.scalasystems.curso.spark.kafka

class Stopwatch {

  private val start = System.currentTimeMillis()

  override def toString() = (System.currentTimeMillis() - start) + " ms"

}
