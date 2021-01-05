package com.fengjr.NeographX

import org.apache.spark.graphx.{Edge, Graph, VertexId, lib}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

object test090829 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Neo4jGraphX")
      .setMaster("local[*]")
      .set("spark.neo4j.bolt.url","bolt://10.10.203.132:7687")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)
    val neo = Neo4j(sc)

    //  从Neo4j中读取数据； 使用janusgraph 结合Spark进行离线计算效果更好；Neo4j+spark 可以做成异步的计算工具，支持一些简单指标的计算
    val neodata = neo.cypher("MATCH (p1:real_income_no) -[:HasPhone]-> (ph1:phone) -[f:HasCall]- (ph2:phone) <-[:HasPhone]- (p2:real_income_no)" +
        "RETURN id(p1) as source, id(p2) as target, f.call_cnt as weight," +
        "p1.end_result as end_result1, p1.dpd7 as dpd71, p1.dpd15 as dpd151, p1.dpd30 as dpd301, p1.dpd90 as dpd901 ," +
        "p2.end_result as end_result2, p2.dpd7 as dpd72, p2.dpd15 as dpd152, p2.dpd30 as dpd302, p2.dpd90 as dpd902 limit 1000").loadRowRdd
    // allNode 存储所有节点，以及节点的属性； 并生成节点NodeRDD
    val allNode = neodata.map(
      x => (x(0),x(3),x(4),x(5),x(6),x(7)))
      .union(neodata.map(
        x => (x(1),x(8),x(9),x(10),x(11),x(12))
      )
    )
    println("此图中所有的节点数目是多少" + allNode.distinct().count().toString)  // 打印所有的节点信息allNode.distinct.foreach(println(_))   去重 allNode.distinct.foreach(println(_))
    val noderdd:RDD[(VertexId,(String,String,String,String,String))] = allNode.distinct.map(
      x => (
        x._1.toString.toLong,
        (x._2.toString,x._3.toString,x._4.toString,x._5.toString,x._6.toString)
      )
    )
    // relations 为节点RDD，存储网络中的所有关系
    val relations:RDD[Edge[String]] = allNode.distinct.map(x =>(Edge(x._1.toString.toLong,x._2.toString.toLong,x._3.toString)) )
    val defaultUser = ("2","3","4","5", "6") //  定义一个默认属性，避免有不存在用户的关系
    // 使用Graph方法，构造图garph
    val graph = Graph(noderdd, relations)
    graph.vertices.top(10).foreach(println(_))
    val nodes = graph.vertices

    // 调用spark.graphx.lib中的LabelPropagation算法包 得到新图
    val LpaGraph = lib.LabelPropagation.run(graph,10)
    LpaGraph.vertices.top(10).foreach(println(_))
//    LpaGraph.triplets.map(
//      triplet =>
//        triplet.srcId +"-"+ triplet.srcAttr + "|" + triplet.attr + "|" + triplet.dstId +"-"+ triplet.dstAttr
//    ).top(10).foreach(println(_))

    // 存储社区划分结果，（不带属性的结果）
    val LpaNodes = LpaGraph.vertices
    val LpaEdges = LpaGraph.edges
    val Lparesult = LpaNodes.map(x => (x._2.toLong ,x._1.toLong)).groupByKey
    Lparesult.take(100).foreach(println(_))
    Lparesult.saveAsTextFile("output0830p1")

    // 进行结果分析
//    val Lpan = LpaNodes.join(nodes)
//    Lpan.top(100).foreach(println(_))
//    Lpan.saveAsTextFile("output0830p2")
//
////    val end = Lpan.map(x => (x._2._1,x._1,x._2._2))
//    val end = Lpan.map{
//      x =>
//        if (x._2._2._1 ==2 & x._2._2._2 ==1){
//          (x._2,x._1,11)
//        }else if(x._2._2._1 ==2 & x._2._2._2 !=1){
//          (x._2,x._1,22)
//        }else if(x._2._2._1 ==1){
//          (x._2,x._1,33)
//        }
//    }
////    end.collect.f?oreach(println(_))
//    end.saveAsTextFile("output0830p3")

    //    val edgeRdd = neodata.map(x => (x(0).toString,x(1).toString ,x(2).toString ))
////    edgeRdd.foreach(println)
//
//
//    val srcNodeSet = neodata.map(x => (x(0),x(2)))
//
//    val desNodeSet = neodata.map(x => (x(1),x(2)))
//    println(edgeRdd.distinct().count)
//    println(srcNodeSet.distinct().count)
//    println(desNodeSet.distinct().count)
////    val distin = edgeRdd.
//    val unionNode = edgeRdd.map(x=>x._1).union(edgeRdd.map(x =>x._2))
//    unionNode.distinct.foreach(println(_))
//
//    println(unionNode.distinct().count())
    sc.stop()
  }
}
