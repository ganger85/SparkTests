/**
 * Created by arincon on 5/03/15.
 */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Main extends App{

  override def main(args: Array[String]) {
  val appName: String = "Test1"

  val master: String = "local[2]"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    var sc=new SparkContext(conf)

    //val z = sc.parallelize(List(1,2,3,4,5,6), 2)
    //val res=z.aggregate(0)(math.max(_, _), _ + _)
    //val z = sc.parallelize(List("a","b","c","d","e","f"),2)
    //val res=z.aggregate("")(_ + _, _+_)
//val res=z.aggregate("x")(_ + _, _+_)
    val z = sc.parallelize(List("12","23","345","4567"),2)
  val res=  z.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)
    println(res)
  }

}
