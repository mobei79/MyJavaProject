package com.fengjr.NeographX

import java.util.Date

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

object LouvainAlgorithm {

  def main(args: Array[String]): Unit = {
    val startTime=new Date()
    val conf = new SparkConf()
      .setAppName("Neo4jGraphX")
      .setMaster("local[*]")    // 本地运行参数配置，提交集群时注释掉。
      .set("spark.neo4j.bolt.url","bolt://10.10.204.24:7687")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)
    val neo = Neo4j(sc)

    val ComplexSocialNet = neo.cypher("""MATCH (p1:real_income_no) -[:HasPhone]-> (ph1:phone) -[f:HasCall]- (ph2:phone) <-[:HasPhone]- (p2:real_income_no) RETURN id(p1) as source, id(p2) as target limit 29""").loadRowRdd
    val relationRdd:RDD[Edge[Int]] = ComplexSocialNet.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,11)) )
    val defaultUser = (79) //  定义一个默认属性，避免有不存在用户的关系
    // 得到Graph ：使用Graph方法，构造图garph
    val graph = Graph.fromEdges(relationRdd,defaultUser)
    graph.vertices.take(10).foreach(println(_))
    println("________________")
    graph.edges.take(10).foreach(println(_))
    val LpaGraph = graph.mapVertices{
      case (vid:VertexId ,attr:(Int)) => vid
    }
    val e = LpaGraph.triplets
    val mid_rst1 = e.map( x=> x.srcId)
    LpaGraph.vertices.take(10).foreach(println(_))
    println("________________")
    LpaGraph.edges.take(10).foreach(println(_))
    sc.stop()
  }
  }
