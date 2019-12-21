package com.ali.ip_total

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
/**
  * 统计实时访问量最高的ip
  */

//输入数据格式
case class ApacheLog(ip:String,userId:String,eventTime:Long,method:String,url:String)
//输出数据格式
case class UrlCount(url:String ,windowEnd:Long,count:Long)
class CountAgg extends AggregateFunction[ApacheLog,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLog, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
class WindowResultFunction extends WindowFunction[Long,UrlCount,String,TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlCount]): Unit = {
    val url=key
    val count=input.iterator.next()
    out.collect(UrlCount(url,window.getEnd,count))
  }
}

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
    }).filter(_.method=="GET")
        .keyBy(_.url)
        .timeWindow(Time.minutes(1),Time.seconds(5))
        .aggregate(new CountAgg(),new WindowResultFunction())
        .keyBy(_.windowEnd)
        .process(new TopNHotUrls(5))
        .print()

    env.execute("url job")

  }
}
class TopNHotUrls(i: Int) extends KeyedProcessFunction[Long,UrlCount,String] {
  lazy val urlState=getRuntimeContext.getListState(new ListStateDescriptor[UrlCount]("urlstate",classOf[UrlCount]))

  override def processElement(value: UrlCount, ctx: KeyedProcessFunction[Long, UrlCount, String]#Context, out: Collector[String]): Unit = {
  urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd+10000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allurlViews:ListBuffer[UrlCount]=ListBuffer()
    import scala.collection.JavaConversions._
    for(urlview<-urlState.get()){
      allurlViews += urlview
    }
    urlState.clear()

    val sortedUrlviews=allurlViews.sortBy(_.count)(Ordering.Long.reverse).take(i)

    val result:StringBuilder=new StringBuilder

    result.append("==============================")
    //错开多长时间
    result.append("时间：").append(new Timestamp(timestamp-10000)).append("\n")
    for(i<-sortedUrlviews.indices){
      val current:UrlCount=sortedUrlviews(i)
      result.append(i+1).append(":  ")
        .append("URL:").append(current.url)
        .append("   流量：").append(current.count).append("\n")
    }

    result.append("==============================\n\n")
    Thread.sleep(500)
    out.collect(result.toString())
  }



}