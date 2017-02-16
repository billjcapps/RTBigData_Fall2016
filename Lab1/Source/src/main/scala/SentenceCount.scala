import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Bill Capps on 2/11/2017
  */
object SentenceCount {

  def main(args: Array[String]) {

    // setup
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    val sparkConf = new SparkConf().setAppName("SentenceCount").setMaster("local[*]");
    val sc = new SparkContext(sparkConf);
    val input = sc.textFile("input");

    // get count
    val sentCnt = input.flatMap(line => line.split('.')).map(sentence => (sentence, 1));
    val temp = sentCnt.reduceByKey(_+_);
    val total = input.map(file => (file, file.split('.').length))
    total.saveAsTextFile("outputCount");

    // output
    val sentOutput = temp.sortByKey(true);
    sentOutput.saveAsTextFile("outputSorted");

  }

}