package com.fengjr.NeographX

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.neo4j.spark.Neo4j

object LPADemoAllRelations {

  /**
    * 定义函数
    * @param args
    */
  def partitionsFun(/*index : Int,*/iter : Iterator[(String,String)]) : Iterator[String]= {
    var woman = List[String]()
    while (iter.hasNext){
      val next = iter.next()
      next match {
        case (_,"female") => woman = /*"["+index+"]"+*/next._1 :: woman
        case _=>
      }
    }
    return woman.iterator
  }

  /**
    * LabelPropagation
    */

  /**
    * main Function
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Neo4jGraphX")
//      .setMaster("local[*]")    // 本地运行参数配置，提交集群时注释掉。
      .set("spark.neo4j.bolt.url","bolt://10.10.204.24:7687")
      .set("spark.neo4j.bolt.user","neo4j")
      .set("spark.neo4j.bolt.password","kgfengjr")
    val sc = new SparkContext(conf)
    val neo = Neo4j(sc)

    //  从Neo4j中读取数据； 使用janusgraph 结合Spark进行离线计算效果更好；Neo4j+spark 可以做成异步的计算工具，支持一些简单指标的计算
//    val neodata = neo.cypher("MATCH pathCommonContacts = (p11:id_card) <-[r_idcard11:HasIdCard]- (income11:real_income_no) -[r_contact11:HasPhone]-> (phone11:phone) 	<-[r_contact12:HasPhone]- (income12:real_income_no) -[r_idcard12:HasIdCard]-> (p12:id_card)  where p11.id_card <> p12.id_card return id(p11) as from ,id(p12) as to union MATCH pathCommonEmergency = (p21:id_card) <-[r_idcard21:HasIdCard]- (income21:real_income_no) -[r_11:HasEmergencyPhone]-> (phone11:phone)  <-[r_22:HasEmergencyPhone]- (income22:real_income_no) -[r_idcard22:HasIdCard]-> (p22:id_card)  where p21.id_card <> p22.id_card return id(p21) as from ,id(p22) as to union  MATCH pathCommonPhoneEmergency = (p21:id_card) <-[r_idcard21:HasIdCard]- (income21:real_income_no) -[r_11:HasPhone]-> (phone11:phone)   	<-[r_22:HasEmergencyPhone]- (income22:real_income_no) -[r_idcard22:HasIdCard]-> (p22:id_card)  where p21.id_card <> p22.id_card  return id(p21) as from ,id(p22) as to union MATCH pathCommonPhoneEmergency = (p21:id_card) <-[r_idcard21:HasIdCard]- (income21:real_income_no) -[r_11:HasContact]-> (phone11:phone)  <-[r_22:HasEmergencyPhone]- (income22:real_income_no) -[r_idcard22:HasIdCard]-> (p22:id_card)  where p21.id_card <> p22.id_card return id(p21) as from ,id(p22) as to union MATCH pathCommonPhoneEmergency = (p21:id_card) <-[r_idcard21:HasIdCard]- (income21:real_income_no) -[r_11:HasPhone]-> (phone11:phone) -[r2:HasCall]-> (phone3:phone)  <-[r_22:HasEmergencyPhone]- (income22:real_income_no) -[r_idcard22:HasIdCard]-> (p22:id_card) where p21.id_card <> p22.id_card return id(p21) as from ,id(p22) as to union MATCH pathCommonPhoneEmergency = (p21:id_card) <-[r_idcard21:HasIdCard]- (income21:real_income_no) -[r_11:HasPhone]-> (phone11:phone) <-[r_22:HasContact]- (income22:real_income_no) -[r_idcard22:HasIdCard]-> (p22:id_card) where p21.id_card <> p22.id_card return id(p21) as from ,id(p22) as to union MATCH pathCommonPhoneEmergency = (p21:id_card) <-[r_idcard21:HasIdCard]- (income21:real_income_no) -[r_11:HasPhone]-> (phone11:phone) -[r2:HasCall]-> (phone3:phone) 	<-[r_22:HasPhone]- (income22:real_income_no) -[r_idcard22:HasIdCard]-> (p22:id_card)  where p21.id_card <> p22.id_card return id(p21) as from ,id(p22) as to limit 10").loadRowRdd
    val neodata = neo.cypher("(P1:id_card) <-[real1:HasIdCard]-(income1:real_income_no) -[r:HasDeviceAddress]-> (DA1:device_address) <-[r2:HasDeviceAddress]- (income2:real_income_no)-[real2:HasIdCard] ->(P2:id_card) where id(P1) <>id(P2) return id(p11) as from ,id(p12) as to").loadRowRdd
    val relationRdd:RDD[Edge[Int]] = neodata.map(x =>(Edge(x(0).toString.toLong,x(1).toString.toLong,1)) )
    println("-------------所有关系的RDD如下所示relationRdd-------------" )
    relationRdd.take(40).foreach(println(_))
    val defaultUser = ("2") //  定义一个默认属性，避免有不存在用户的关系

    // 得到Graph ：使用Graph方法，构造图garph
    val graph = Graph.fromEdges(relationRdd,defaultUser)
    graph.vertices.top(100).foreach(println(_))
    val nodes = graph.vertices

    val LpaGraph = Labelpropagation.run(graph,10)//lib.LabelPropagation.run(graph,10)
    // 得到LpaGraph：调用spark.graphx.lib中的LabelPropagation算法包 得到新图
    //    val LpaGraph = lib.LabelPropagation.run(graph,10)
    //    LpaGraph.vertices.top(10).foreach(println(_))
    LpaGraph.triplets.map(
      triplet =>
        triplet.srcId +"-"+ triplet.srcAttr + "|" + triplet.attr + "|" + triplet.dstId +"-"+ triplet.dstAttr
    ).top(40).foreach(println(_))
    println("-------------以上是运行LPA算法之后的新图的triplet-------------" )

    /**
      * 新的map方法
      */
    // 存储社区划分结果，（不带属性的结果）
    val LpaNodes = LpaGraph.vertices    // (5411584,5411584) 即(Vid , Cid)
    val LpaEdges = LpaGraph.edges
    val Lparesult = LpaNodes.map(x => (x._2.toLong ,x._1.toLong)).groupByKey
    Lparesult.take(100).foreach(println(_))   //(6311410,CompactBuffer(2701784, 4451802, 80217))
    Lparesult.saveAsTextFile("output0918Allrelationp1")
    println("-------------存储社区划分结果，（不带属性的结果）格式为(6311410,CompactBuffer(2701784, 4451802, 6311410, 4356646, 3225776, 6032975, 4437285, 6334659, 2280217))-------------" )

    val Lpan = LpaNodes.join(nodes)   // (5411584,(5411584,(1,0))) 即 （Vid ，（Cid ，Pid））
    Lpan.top(100).foreach(println(_))
    val LpaRstFull = Lpan.map(t => (t._2._1,1)).reduceByKey(_+_)
    LpaRstFull.take(100).foreach(println(_))
    println("-------------统计各个社区的顶点个数-------------" )
    Lparesult.saveAsTextFile("output0918Allrelationp2")
    sc.stop()
  }
}
