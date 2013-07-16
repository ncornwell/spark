package spark.streaming.examples

import spark._
import spark.streaming.{DStream, Seconds, StreamingContext}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path

object FXLogisticRegression {

  def main(args: Array[String]) {
    val ssc = new StreamingContext(System.getenv("MASTER"), "FXStream", Seconds(30),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    import scala.math.exp
    import java.text.SimpleDateFormat
    import java.util._

    val textFile = ssc.fileStream[LongWritable, Text, TextInputFormat](
      "hdfs://db1.stg:9000/fix/stream/marketdata/",
      (p:Path) => !p.getName.contains("tmp"),
      newFilesOnly = true)
      .map(_._2.toString)

    val format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))

    case class MarketData(bid: Double, offer: Double, time: Long)

    val marketData: DStream[MarketData] = textFile
      .filter(line => line.contains("35=X"))
      .map(line => {
      val time = format.parse(line.substring(line.indexOf("52=") + 3, line.indexOf("\001262="))).getTime
      val symbol = line.substring(line.indexOf("55=") + 3, line.indexOf("\001270="))

      val prices = line.split("279=")
        .drop(1)
        .map(l => {
        val price = l.substring(l.indexOf("270=") + 4, l.indexOf("\00115="))
        val bidOrOffer = l.substring(l.indexOf("269=") + 4, l.indexOf("\001278="))

        (price.toDouble, bidOrOffer.toInt)
      })

      val bid = prices.find(_._2 == 0).getOrElse(1.0 -> 1)
      val offer = prices.find(_._2 == 1).getOrElse(1.0 -> 1)

      symbol -> MarketData(bid._1, offer._1, time)
    }).filter(_._1 == "EUR/USD").map(_._2)

    val deltas = marketData.glom().flatMap(e => {
      val timeData = e.sortBy(_.time).sliding(2).filter(_.length == 2).map(seq => math.log(seq(1).bid / seq(0).bid)).filter(_ != Double.NaN).toList

      for {
        i <- 0 until timeData.length - 3
      } yield (if (timeData(i + 3) > 0.0) 1.0 else -1.0, spark.util.Vector(timeData(i), timeData(i + 1), timeData(i + 2)))
    })

    val rand = new Random(42)
    var w = spark.util.Vector(3, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    deltas.foreach(d => {
      for (i <- 1 to 5) {
        println("On iteration " + i)
        val gradient = d.map {
          p =>
            (1 / (1 + exp(-p._1 * (w dot p._2))) - 1) * p._1 * p._2
        }.aggregate(spark.util.Vector(0.0, 0.0, 0.0))(_ + _, _ + _)
        w -= gradient
      }

      val rme = d.map(i => {
        val res = i._1 - (i._2 dot w)
        res * res
      }).aggregate(0.0)(_ + _, _ + _) / d.count

      val rmse = math.sqrt(rme)

      println("New Values: " + d.count())
      println("Weights: " + w)
      println("Error: " + rmse)

      val buySell = if ((d.first()._2 dot w) > 0.0) "BUY!!!!" else "SELL!!!!"
      println(buySell)
      println(buySell)
      println(buySell)
    })




    ssc.start()

  }

}
