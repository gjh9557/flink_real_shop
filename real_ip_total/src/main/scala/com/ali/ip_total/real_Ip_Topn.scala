package com.ali.ip_total

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * 统计实时访问量最高的ip
  */

//输入数据格式
case class ApacheLog(ip:String,userId:String,eventTime:Long,method:String,url:String)
//输出数据格式
case class UrlCount(url:String ,windowEnd:Long,count:Long)

object real_Ip_Topn {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.readTextFile("E:\\qqq\\days\\12.20\\real_shop\\real_ip_total\\src\\main\\resources\\apache.log")
      .map(line => {
        val arr = line.split(" ")
        //定义时间转换模板转成时间戳
        val simpleDateFormat = new SimpleDateFormat(Constant.constant.dateformat)
        val timestamp = simpleDateFormat.parse(arr(3)).getTime
        ApacheLog(arr(0), arr(1), timestamp, arr(5), arr(6))
      })
      //乱序的方式创建时间错和水位
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.seconds(10)) {
      override def extractTimestamp(t: ApacheLog): Long = {
        t.eventTime

      }
    })

    env.execute("url job")

  }
}
