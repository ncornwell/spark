package spark.streaming.examples

import spark._
import spark.streaming.{DStream, Seconds, StreamingContext}

object FXLogisticRegression {

  def main(args: Array[String]) {
    val ssc = new StreamingContext("spark://db1.stg:7070", "FX", Seconds(10),
      System.getenv("SPARK_HOME"), Seq(System.getenv("SPARK_EXAMPLES_JAR")))

    import scala.math.exp
    import java.text.SimpleDateFormat
    import java.util._

    val textFile = ssc.textFileStream("hdfs://db1.stg:9000/fix/stream/marketdata/")

    val format = new SimpleDateFormat("yyyyMMdd-HH:mm:ss")
    format.setTimeZone(TimeZone.getTimeZone("GMT"))

    case class MarketData(bid: Double, offer: Double, time: Long)

    val marketData:DStream[MarketData] = textFile
      .filter(line => line.contains("35=X") && line.contains("EUR/USD"))
      .map(line => {
      val time = format.parse(line.substring(line.indexOf("52=") + 3, line.indexOf("\001262="))).getTime

      val prices = line.split("279=")
        .drop(1)
        .map(l => {
        val price = l.substring(l.indexOf("270=") + 4, l.indexOf("\00115="))
        val bidOrOffer = l.substring(l.indexOf("269=") + 4, l.indexOf("\001278="))

        (price.toDouble, bidOrOffer.toInt)
      })

      val bid = prices.find(_._2 == 0).getOrElse(1.0 -> 1)
      val offer = prices.find(_._2 == 1).getOrElse(1.0 -> 1)

      MarketData(bid._1, offer._1, time)
    }).window(Seconds(30))

    val deltas = marketData.glom().flatMap(e => {
      val timeData = e.sortBy(_.time).sliding(2).filter(_.length == 2).map(seq => math.log(seq(1).bid / seq(0).bid)).filter(_ != Double.NaN).toList

      for {
        i <- 0 until timeData.length - 3
      } yield (if (timeData(i + 3) > 0.0) 1.0 else -1.0, spark.util.Vector(timeData(i), timeData(i + 1), timeData(i + 2)))
    })

    val rand = new Random(42)



    deltas.foreach(d => {
      var w = spark.util.Vector(3, _ => 2 * rand.nextDouble - 1)
      println("Initial w: " + w)
      for (i <- 1 to 5) {
        println("On iteration " + i)
        val gradient = d.map {
          p =>
            (1 / (1 + exp(-p._1 * (w dot p._2))) - 1) * p._1 * p._2
        }.reduce(_ + _)
        w -= gradient
      }

      val rme = d.map(i => {
        val res = i._1 - (i._2 dot w)
        res * res
      }).reduce(_ + _) / d.count

      val rmse = math.sqrt(rme)

      println(rmse)
    })




    ssc.start()

  }

}
