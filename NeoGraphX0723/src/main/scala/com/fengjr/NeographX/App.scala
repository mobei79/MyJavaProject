package com.fengjr.NeographX

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object App {


  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val conf = new SparkConf().setAppName("guoge").setMaster("local[*]")   //实例化 spark的配置项
    conf.set("spark.neo4j.bolt.url","bolt://10.10.204.30:7687")
    conf.set("spark.neo4j.bolt.user","neo4j")
    conf.set("spark.neo4j.bolt.password","neo4j")
    val sc = new SparkContext(conf)   // 实例化SparkContext
//    val neo = Neo4j(sc)   // 实例化Neo4j对象

    val lines =  sc.textFile("D:\\software\\NeoGraphX0723\\guo.txt")       // 读取本地文件
    val pairs = lines.map(s=>(s,1))
    pairs.reduceByKey((x, y)=>x+y).foreach(println)

    sc.stop()
    println( "Hello World!" )
  }
}

//class test722p2 {
//  val spark = SparkSession.builder().appName("play").master("local[*]")
//    .config("spark.neo4j.bolt.url","bolt://10.10.204.30:7687")
//    .config("spark.neo4j.bolt.user","neo4j")
//    .config("spark.neo4j.bolt.password","neo4j")
//    .getOrCreate()
//  val neo = Neo4j(spark.sparkContext)

// 使用sparkconf配置
//  val conf = new SparkConf().setAppName("neo4j").setMaster("local[*]")
//    .set("spark.neo4j.bolt.url","bolt://10.10.204.30:7687")
//    .set("spark.neo4j.bolt.user","neo4j")
//    .set("spark.neo4j.bolt.password","neo4j")
//  val sc = new SparkContext(conf)
//  val neo = Neo4j(sc)

//通过 Neo4jConfig 来做配置
//  val sparkSession1 = SparkSession.builder().master("local[*]").appName("LoadDataToNeo4j")
//    .getOrCreate();
//  val sc = sparkSession1.sparkContext
//  val config = Neo4jConfig("bolt://10.10.204.30:7687","neo4j",Option("neo4j"))
//  Neo4j(sc).cypher("CREATE (c:Client {id:1230}) return c").loadRdd
//  sparkSession1.close()
//}