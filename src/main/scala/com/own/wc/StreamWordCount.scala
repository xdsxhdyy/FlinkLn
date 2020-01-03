package com.own.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * @Author luoyu
  * @create 2019/12/9 21:24
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    //流的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val testDataStream = env.socketTextStream(host,port)

    //空格切分
    val wdDataStream = testDataStream.flatMap( _.split("\\s"))
      .filter(_.nonEmpty)
      .map( (_, 1) )
      .keyBy(0)
      .sum(1)

    //设置并行度打印
    wdDataStream.print().setParallelism(1)

    //启动flink，执行任务
    env.execute("stream word count")
  }

}
