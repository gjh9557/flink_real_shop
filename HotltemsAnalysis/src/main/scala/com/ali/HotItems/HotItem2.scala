package com.ali.HotItems

//import org.apache.flink.api.common.state.StateTtlConfig.TimeCharacteristic
import java.sql.Timestamp
import java.util.Properties

import Constant.Constant
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 在1的基础上将数据存到kafka,在从kafka那个主题来读取数据。
  */
object HotItem2 {
  def main(args: Array[String]): Unit = {
    val properties=new Properties()
    properties.setProperty("bootstrap.servers",Constant.bootstarp_servers)
    properties.setProperty("group.id",Constant.group_id)
    properties.setProperty("key.deserializer",Constant.deserializer)
    properties.setProperty("value.deserializer",Constant.deserializer)
    properties.setProperty("auto.offset.reset",Constant.offset_reset)


    //初始化环境：
  val env=StreamExecutionEnvironment.getExecutionEnvironment
//显示的定义time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream=env.
    //从kafka中读数据
      addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
//      readTextFile("E:\\qqq\\days\\12.20\\real_shop\\HotltemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
      UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
      })
      //指定时间戳和watermark这里的数据是升序数据可以用下面这个算子来提高效率。这里用的是毫秒，所以时间戳* 1000
      .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(x=>{"pv".equals(x.behavior)})
        .keyBy("itemId")
      //创建窗口，这里第一个窗口是运行的全部时间，第二个参数是滑动步长
        .timeWindow(Time.hours(1),Time.minutes(5))

        .aggregate( new CountAgg(), new WindowResultFunction())
        .keyBy("windowEnd")
        .process(new TopNHotItems(3))
        .print()

    env.execute("hot item job")

  }
}
//自定义实现process function,这里的string是为了输出
class TopNHotItems(i: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{
  //定义状态ListState
  private var itemState:ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
val itemStateDesc =new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
itemState=getRuntimeContext.getListState(itemStateDesc)
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add(i)
    //注册定时器，触发时间定位 windwoEnd + 1 ，触发时说明windwo已经收集完成所有数据
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }
//定时器触发操作，从state里取出所有数据，排序取Topn,输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    super.onTimer(timestamp, ctx, out)
    //获取所有的商品点击信息
    val allItem:ListBuffer[ItemViewCount]=ListBuffer()
    import scala.collection.JavaConversions._
    for(item <- itemState.get()){

allItem+=item
    }
    //清除状态中的数据，释放空间
    itemState.clear()

    //根据点击量从大到小排序，选取topn
    val sortItems = allItem.sortBy(_.count)(Ordering.Long.reverse).take(i)

    //将排名数据格式化，便于打印输出
    val result=new StringBuilder

    result.append("=========================================\n")

    result.append("时间").append(new Timestamp(timestamp -1)).append("\n")

    for(i <- sortItems.indices){
      val currentItem=sortItems(i)

      //输出打印格式
      result.append(i+1).append(":").append("id: ").append(currentItem.Itemid).append("\t").append("count: ").append(currentItem.count)
        .append("\n")


    }
    result.append("=======================================\n\n")

    Thread.sleep(1000)

    out.collect(result.toString())

  }

}
//自定义实现聚合函数
class CountAgg extends AggregateFunction[UserBehavior,Long,Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc+acc1
}
//自定义实现window function 输出ItemViewCOunt格式
//这里windowfunction中的四个参数分别是：input,output,key,TimeWindow. 里面的key就是前面keyby的字段，当keyby里面穿的是一个“a”这样去分组的时候，这里必须是tuple，可以通过_.a这样可以直接指定为a的类型如：long来替代toupl
class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId=key.asInstanceOf[Tuple1[Long]].f0
    val count=input.iterator.next()
    out.collect(ItemViewCount(itemId,window.getEnd,count))
  }
}

//输入样例类
case class UserBehavior(userid:Long,itemId: Long,categoryId: Int,behavior:String,timestamp:Long)
//输出样例类
case class ItemViewCount(Itemid:Long,windowEnd:Long,count:Long)