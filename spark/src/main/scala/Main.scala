import org.apache.spark.{SparkConf, SparkContext}

import org.apache.hadoop.fs.{FileSystem, Path}

object Main extends App {

  override def main(args: Array[String]): Unit = {
    val sc = getSparkContext

    avg(sc, args)
  }

  def getSparkContext: SparkContext = {
    // Spark 1
    val conf = new SparkConf().setAppName("Spark avg views per category")
    new SparkContext(conf)

    // Spark 2
    //sval spark = SparkSession.builder.appName("Exercise 302 - Spark2").getOrCreate()
    //spark.sparkContext
  }

  def avg(sc: SparkContext, args: Array[String]): Unit = {
    val reddNationVideos = sc.textFile("hdfs:"+args(0)+"/*.csv").map(VideoData.extract)
    val rddCategory = sc.textFile("hdfs:"+args(1)).map(CategoryData.extract)

    // Average views for every category
    val rddNations = reddNationVideos
      .filter(_.categoryId >= 0)
      .map(x => (x.categoryId, x.view_count))
      .aggregateByKey((0.0,0.0))((a,v)=>(a._1+v,a._2+1),(a1,a2)=>(a1._1+a2._1,a1._2+a2._2))
      .map({case(k,v)=>(k,(v._1/v._2).toInt)})
      .cache()

    // Category name for every category id
    val rddC = rddCategory
      .map(v => (v.id, v.category))
      .cache()

    val fs = FileSystem.get(sc.hadoopConfiguration)
    val outPutPath = new Path("hdfs:"+args(2))
    if (fs.exists(outPutPath)) fs.delete(outPutPath, true)

    // Join by category id and then sort by average
    rddNations
      .join(rddC)
      .map({case(_,v)=>(v._2, v._1)})
      .sortBy(-_._2)
      .saveAsTextFile("hdfs:"+args(2))
  }

}