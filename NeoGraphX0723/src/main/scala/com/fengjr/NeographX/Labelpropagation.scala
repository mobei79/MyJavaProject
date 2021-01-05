package com.fengjr.NeographX

import org.apache.spark.graphx.{Graph, VertexId, _}

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag
object Labelpropagation {
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], maxSteps: Int): Graph[VertexId, ED] = {
    require(maxSteps > 0, s"Maximum of steps must be greater than 0, but got ${maxSteps}")

    val lpaGraph = graph.mapVertices { case (vid: VertexId,attr :(Int)) => vid }  // 使用graph.mapVertices 方法改变节点的属性值,将属性值变为节点的唯一标签
    def sendMessage(e: EdgeTriplet[VertexId, ED]): Iterator[(VertexId, Map[VertexId, Long])] = {
      Iterator((e.srcId, Map(e.dstAttr -> 1L)), (e.dstId, Map(e.srcAttr -> 1L)))
    }
    def mergeMessage(count1: Map[VertexId, Long], count2: Map[VertexId, Long])
    : Map[VertexId, Long] = {
      // Mimics the optimization of breakOut, not present in Scala 2.13, while working in 2.12
      val map = mutable.Map[VertexId, Long]()
      (count1.keySet ++ count2.keySet).foreach { i =>
        val count1Val = count1.getOrElse(i, 0L)
        val count2Val = count2.getOrElse(i, 0L)
        map.put(i, count1Val + count2Val)
      }
      map
    }
    def vertexProgram(vid: VertexId, attr: Long, message: Map[VertexId, Long]): VertexId = {
      if (message.isEmpty) attr else message.maxBy(_._2)._1
    }
    val initialMessage = Map[VertexId, Long]()
    Pregel(lpaGraph, initialMessage, maxIterations = maxSteps)(
      vprog = vertexProgram,
      sendMsg = sendMessage,
      mergeMsg = mergeMessage)
  }
}
