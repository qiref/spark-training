package com.spark.neo4j

import org.apache.spark.sql.SparkSession
import org.neo4j.spark.Neo4j

/**
  * author YaoQi
  * Date: 2019/4/27 18:20
  * Description: spark 集成neo4j
  */
object SparkToNeo4j {

  def main(args: Array[String]): Unit = {
    val appName = "sparkToNeo4j"
    val master = "local[3]"
    val sparkSession = SparkSession.builder().appName(appName).master(master)
      .config("spark.neo4j.bolt.url", "192.168.64.122:7687")
      .config("spark.neo4j.bolt.user", "neo4j")
      .config("spark.neo4j.bolt.password", "123456").getOrCreate()

    val neo = Neo4j(sparkSession.sparkContext)


    sparkSession.close()
  }
}
