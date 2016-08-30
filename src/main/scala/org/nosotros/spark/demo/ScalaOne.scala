package org.nosotros.spark.demo

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector.toSparkContextFunctions

/**
  * Created by marcos on 28/08/16.
  */
class ScalaOne {

  val MASTER_URL: String = "local[4]"
  val APP_NAME: String = "nosotros.demo"
  val CASSANDRA_NODES: String = "spark.cassandra.connection.host"
  val CASSANDRA_ADDRESSES = "192.168.1.70"

  val configuration = new SparkConf(true)
    .setAppName(APP_NAME)
    .setMaster(MASTER_URL)
    .set(CASSANDRA_NODES, CASSANDRA_ADDRESSES)

  val sc = new SparkContext(configuration)

}

object ScalaOne{
  val TABLE_NAME: String = "t"
  val KEY_SPACE: String = "k"

  def main(args: Array[String]): Unit = {
    process(demoApp)
  }

  def demoApp: ScalaOne = { new ScalaOne}

  def process(one: ScalaOne) = {

    println (one.sc.sparkUser)
    println (one.sc.startTime)

    val rdd = toSparkContextFunctions(one.sc) cassandraTable(KEY_SPACE, TABLE_NAME)
    rdd.foreach( println ( _ ))
  }
}
