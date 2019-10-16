package Utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 725841 on 2019/10/15.
  */
class SparkUtils {

}
object SparkUtils{
  /**
    * 获取本地sc
    * @param appName
    * @return
    */
  def getLocalSC(appName:String): SparkContext ={
    var conf = new SparkConf()
    conf.setMaster("local").setAppName(appName)
    var sc = new SparkContext(conf)
    sc
  }
}
