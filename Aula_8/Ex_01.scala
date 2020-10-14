cd /opt/polynote;nohup ./polynote.py &	



cd /opt/polynote;nohup ./polynote.py &

package com.mycompany

object ProcessFiles{

}

curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
sudo yum install sbt

package com.mycompany
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessFiles {

    def main(args: Array[String]): Unit = {
      if (args.length < 1) {
        println("Precisa especificar o arquivo")
        System.exit(1)
      }

      val spark = SparkSession.builder
        .appName("Process Files")
        .getOrCreate()

      var file = ""
      for(file <- args){
        val fileDF = spark.read.text(file)
        val count = fileDF.count()
        println("### %s: count %,d".format(file, count))
      }

      println("Pressione Enter para sair")
      val line = Console.readLine()
    }
}


cp spark-defaults.conf.template spark-defaults.conf

subir maquina slave:  ./start-slave.sh -c 1 -m 2G spark://18.216.31.146:7077