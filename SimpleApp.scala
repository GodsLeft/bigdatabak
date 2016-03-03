import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp{
    def main(args: Array[String]){
        val logFile = "logfile"
        val conf = new SparkConf().setAppName("SparkApp")
        val sc = new SparkContext(conf)
        val logData = sc.textFile(logFile, 2).cache()
        val numAs = logData.filter(line=>line.contains("a")).count()
        val numBs = logData.filter(line=>line.contains("b")).count()
        //val numHello = logData.flatMap(x=>x.split(" ")).filter(_.equals("hello")).count()
        val numHello = logData.flatMap(x=>x.split(" ")).filter(_.equals("hello")).collect()
        numHello.foreach(println)
        println("a:%s, b:%s , hello:%s".format(numAs, numBs, numHello))
    }
}
