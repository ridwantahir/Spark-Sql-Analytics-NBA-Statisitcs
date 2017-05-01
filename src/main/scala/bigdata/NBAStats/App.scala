package bigdata.NBAStats

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    /*
     * val conf = new SparkConf()
             .setMaster("local[2]")
             .setAppName("CountingSheep")
			val sc = new SparkContext(conf)
     */
    val spark=SparkSession.builder().appName("NBAStats").master("local[9]").getOrCreate();
    val processor=new NameExtractor();
    processor.apply(spark);
  }

}
