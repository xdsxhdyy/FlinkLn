package com.own.wc

import org.apache.flink.api.scala._

/**
  * @Author luoyu
  * @create 2019/12/9 21:06
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建批处理的执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inpath = "D:\\coding\\API\\4.1\\FlinkLn\\src\\main\\resources\\wordcount.txt"
    //读数据
    val inputDataSet = env.readTextFile(inpath)

    //对dataset进行word count处理
    val wcDataSet = inputDataSet.flatMap( _.split(" ") )
      .map( (_, 1) )
      .groupBy(0)
      .sum(1)

    wcDataSet.print()
  }
}
