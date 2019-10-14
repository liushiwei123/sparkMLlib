package MLib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by 725841 on 2019/10/11.
  */
class BasicStatistics {

}

object BasicStatistics{
  def main(args: Array[String]): Unit = {
    //    summaryStatistics()
    //    correlationsTest()
    //    stratifiedSamplingTest()
    hypothesisTestingTest()
  }
  def summaryStatistics(): Unit ={
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    var vectorsRdd = MLUtils.loadVectors(sc,"data/localVectors")
    vectorsRdd.collect().foreach(println(_))
    val  multivariateStatisticalSummary = Statistics.colStats(vectorsRdd)
    println("均值向量："+multivariateStatisticalSummary.mean)
    println("方差向量："+multivariateStatisticalSummary.variance)
    println("非零值个数向量："+multivariateStatisticalSummary.numNonzeros)
    println("最大值向量："+multivariateStatisticalSummary.max)
    println("最小值向量："+multivariateStatisticalSummary.min )
    //    L0范数是指向量中非0的元素的个数。(L0范数很难优化求解)
    //    L1范数是指向量中各个元素绝对值之和
    //    L2范数是指向量各元素的平方和然后求平方根
    //    L1范数可以进行特征选择，即让特征的系数变为0.
    //    L2范数可以防止过拟合，提升模型的泛化能力，有助于处理 condition number不好下的矩阵(数据变化很小矩阵求解后结果变化很大)
    println("L1范数向量："+multivariateStatisticalSummary.normL1)
    println("L2范数向量："+multivariateStatisticalSummary.normL2)
  }

  //相关系数  1.pearson 皮尔逊相关系数  系数的值为1意味着X和Y可以很好的由直线方程来描述，所有的数据点都很好的落在一条直线上，
  // 且x随着y 的增加而增加。系数的值为−1意味着所有的数据点都落在直线上，且 随着x 的增加而y减少。若数据中存在离群点，影响很大
  //          2.Spearman 斯皮尔曼等级相关系数

  //  1、pearson相关通常是用来计算等距及等比数据或者说连续数据之间的相关的，这类数据的取值不限于整数，如前后两次考试成绩的相关就适合用pearson相关。
  //  2、spearman相关专门用于计算等级数据之间的关系，这类数据的特点是数据有先后等级之分但连续两个等级之间的具体分数差异却未必都是相等的，
  // 比如第一名和第二名的分数差就未必等于第二名和第三名的分数差
  def correlationsTest(): Unit ={
    println("相关性判断")
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    val lineRDD = sc.textFile("data/correlationsData")
    val seriesX = lineRDD.map((_.split(" ").apply(0).toDouble))
    val seriesY = lineRDD.map(_.split(" ").apply(1).toDouble)
    //   默认是皮尔逊相关系数
    val pearson = Statistics.corr(seriesX,seriesY,"pearson")
    val spearman = Statistics.corr(seriesX,seriesY,"spearman")
    println("pearson--->"+pearson)
    println("Spearman--->" +spearman)
  }

  def stratifiedSamplingTest(): Unit ={
    println("分层抽样")
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    val lineRDD = sc.textFile("data/correlationsData")
    println("各层数据量统计")
    lineRDD.map(line=>{
      (line.split(" ").apply(0),1)
    }).reduceByKey(_+_).collect().foreach(println(_))
    val kvRDD = lineRDD.map(line=>{
      (line.split(" ").apply(0),line.split(" ").apply(1).toDouble)
    })
    val fractions =Map("1"->0.2,"2"->0.5,"3"->0.3)
    //    sampleByKey 样本个数和 fractions中的占比并不完全一样，在附近浮动
    val sample1 =  kvRDD.sampleByKey(withReplacement = false,fractions = fractions)
    //    sampleByKeyExact 样本个数是完全根据 fractions 中的百分比来的
    val sample2 =  kvRDD.sampleByKeyExact(withReplacement = false,fractions = fractions)

    println("====================sampleByKey==========================")
    sample1.collect().foreach(println(_))
    println("=================sampleByKey各层样本统计==================")
    sample1.map(value => {
      (value._1,1)
    }).reduceByKey(_+_).foreach(println(_))

    println("======================sampleByKeyExact========================")
    sample2.collect().foreach(println(_))
    println("=================sampleByKeyExact各层样本统计==================")
    sample2.map(value => {
      (value._1,1)
    }).reduceByKey(_+_).foreach(println(_))
  }


  def hypothesisTestingTest(): Unit ={
    println("假设检验")
    val dv: Vector = Vectors.dense(1,2,3,4,5,6,34,23,343,5,6,67,8,86,5,4,3)

    var conf = new SparkConf()
    conf.setMaster("local[2]").setAppName("MLTest")
    var sc = new SparkContext(conf)
    // 数据文件中下标是从1开始的，不是从0开始 如：1 1:4 2:7
//    val examples = MLUtils.loadLibSVMFile(sc,"data/sparseData")
    //    卡方检验  1.入参为向量，则是拟合度检验  2 入参为矩阵，则是独立性检验
//    情况一 ：入参为向量，则是拟合度检验
//            statistic值计算逻辑：sum(((Xi - X均值)*(Xi - X均值))/X均值)
    val goodnessOfFitTestResult  = Statistics.chiSqTest(dv)

//    goodnessOfFitTestResult
    println(goodnessOfFitTestResult )

  }
}