package com.fengjr.NeographX

import java.util.Date

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark._

object LpaPrapatationTest1 {
  def main(args: Array[String]): Unit = {
    val startTime = new Date()

    val conf = new SparkConf().setAppName("graphxNeo4j").setMaster("local[*]")
      .set("spark.neo4j.bolt.url","bolt://10.10.203.132:7687")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)

    val neo = Neo4j(sc)


    ///// 加载 RDDS
    val neo4jdata = neo.cypher("MATCH (p1:real_income_no) -[:HasPhone]-> (ph1:phone) -[f:HasCall]- (ph2:phone) <-[:HasPhone]- (p2:real_income_no) " +
      "RETURN id(p1) as source, id(p2) as target, f.call_cnt as weight,p2.end_result as end_result, p2.dpd7 as dpd7 ,p2.dpd15 as dpd15,p2.dpd30 as dpd30,p2.dpd90 as dpd90 limit 100").loadNodeRdds
//      .partitions(5).batch(10000)
    neo4jdata.foreach(println)

    // Graphx中的节点ID只能是Long型的。
    val edgeRdd = neo4jdata.map(x => (x(0).toString ,x(1).toString,x(2),x(3),x(4),x(5),x(6),x(7)))
        .map(x =>{Edge(x._1.toLong,x._2.toLong,(x._3,x._4,x._5,x._6,x._7,x._8))})//{Edge(x._1,x._2,x._3)})
    edgeRdd.foreach(println)

    val g = Graph.fromEdges(edgeRdd,defaultValue = 3)
    g.vertices.foreach(println)
      // 提取某些节点
//    val neo4jdata = neo.cypher("match (n:real_income_no) return id(n) as id").loadRowRdd
//    neo4jdata.first.schema.fieldNames
//    neo4jdata.first.schema("id")
//    neo4jdata.foreach(println)
//    neo4jdata.take(1).foreach(println)

    // 提取某些节点，将节点存储为Long类型
//    val neo4jdata = neo.cypher("match (n:real_income_no) return id(n)").loadRdd[Long]
//    neo4jdata.foreach(println)

    // 通过传参的方式，限定返回的数据
//    val neo4jdata = neo.cypher("match (n:real_income_no) where id(n) <= {maxId} return id(n)").param("maxId",30).loadRowRdd
//    val neoDataCount = neo4jdata.count()
//    neo4jdata.foreach(println)
//    println(neoDataCount)

    // 提供分区和批大小 -----nodes()方法好像存在一些问题
//    val neo4jdata = neo.nodes("match (n:real_income_no) return id(n) SKIP {_skip} LIMIT {_limit}").partitions(4).batch(25).loadRowRdd
//    neo4jdata.foreach(println)

    //通过pattern 模式加载数据
//    val neo4jdata = neo.pattern("real_income_no",Seq("HasIdCard"),"id_card").rows(80).batch(21).loadNodeRdds
//    neo4jdata.foreach(println)

    // 加载 DATA Frames
//    val neo4jdata = neo.cypher("match (n:real_income_no) return id(n)").loadDataFrame.count()
//    val df = neo.pattern("Person",Seq("KNOWS"),"Person").partitions(12).batch(100).loadDataFrame()
//    println(neo4jdata)
//    println(df)

    // 加载 GraphX Graphs
//    val graphQuery = "match (n:real_income_no)-[r:HasPhone]-(m:phone) return id(n) as source, id(m) as target,type(r) as value"
//    val graph: Graph[Long,String] = neo.rels(graphQuery).loadGraph
//    graph.edges.take(11)
//    println(graph.vertices.count)

//    val graph = neo.pattern(("real_income_no","channel_code"),("HasPhone","create_tm"),("phone","phone")).partitions(2).batch(10).loadGraph[String,String]
//    val graph2 = PageRank.run(graph, 5)
//    graph2.vertices.take(3)
//    println(graph2.vertices.count)
//    graph2.edges.foreach(println)
  }

}
