

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mayanka on 09-Sep-15.
  * Modified by Bill Capps on 2/1/17
  */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input")

    val wc=input.flatMap(line=>{line.split(",")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    /*
    output.saveAsTextFile("output")
    */

    val o=output.collect()

    var x = 0
    var y = 0

    o.foreach{case(word,count)=>{

      word match {
        case "up" => y=y+count
        case "down" => y=y-count
        case "left" => x=x-count
        case "right" => x=x+count
      }

    }}

    val s:String="("+x+","+y+")"

    scala.tools.nsc.io.File("Output").writeAll(s)
  }

}
