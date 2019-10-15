package MLlib

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils

import scala.util.Random

/**
  * Created by 725841 on 2019/10/9.
  */
class DataTypes {

}

object DataTypes{
  def main(args: Array[String]): Unit = {

  }
}
//本地向量
object DataTypesLocalVectors{
  def main(args: Array[String]): Unit = {
    //    稠密向量
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
    //    println(dv)
    //  稀疏向量  向量size 下标对应的数组 非零值对应的数组
    val sv1 = Vectors.sparse(3,Array(0,2),Array(1,3))
    println(sv1.apply(0))
    println(sv1.apply(1))
    println(sv1.apply(2))
    println("==========================")
    //    (k,v)形式赋值
    val sv2 = Vectors.sparse(3,Seq((0,3.0),(2,4.0)))
    println(sv2.apply(0))
    println(sv2.apply(1))
    println(sv2.apply(2))
  }
}

//标记点向量  给向量打标记
object DataTypesLabeledPoint{
  def main(args: Array[String]): Unit = {
    val pos = LabeledPoint(1,Vectors.dense(1,0,3))
    val neg = LabeledPoint(0,Vectors.sparse(3,Array(0,2),Array(1,1)))
// 解析数据
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
// 数据文件中下标是从1开始的，不是从0开始 如：1 1:4 2:7
    val examples = MLUtils.loadLibSVMFile(sc,"data/sparseData")

    println(examples)
  }
}
// 本地矩阵
object DataTypesLocalMatrix{
  def main(args: Array[String]): Unit = {
//    稠密矩阵
    var dm = Matrices.dense(3,2,Array(5,3,2,5,6,7))
    println(dm)
//    稀疏矩阵 csc 列式存储  第一个array是指每一列的第一个非零值在values(即最后一个入参)中的下标
    var sm = Matrices.sparse(3,3,Array(0,1,3,4),Array(0,0,1,2),Array(100,10,1,0.1))
    println(sm)
//    val sparseMatrix= Matrices.sparse(3, 3, Array(0, 2, 3, 6), Array(0, 2, 1, 0, 1, 2), Array(1.0, 2.0, 3.0, 4.0, 5.0, 6.0))
//    println(sparseMatrix)
  }
}
//分布式矩阵
object DataTypesDistributedMatrix{
  def main(args: Array[String]): Unit = {
    blockMatrixTest()

  }
  def rowMatrixTest(): Unit ={
    val sv1 = Vectors.sparse(3,Array(0,2),Array(1,3))
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    var vectorsRdd = MLUtils.loadVectors(sc,"data/localVectors")
    val matrix = new RowMatrix(vectorsRdd)
    println("rows--->"+matrix.numRows())
    println("cols--->"+matrix.numCols())
    val qr = matrix.tallSkinnyQR(true)
    println("qr------>"+qr)
    println("Q--->"+qr.Q)
    println("R--->"+qr.R)
    vectorsRdd.collect().foreach(println(_))
  }

  def indexedRowMatrixTest(): Unit ={
    val sv1 = Vectors.sparse(3,Array(0,2),Array(1,3))
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    var vectorsRdd = MLUtils.loadVectors(sc,"data/localVectors")
    val indexVectorRdd = vectorsRdd.map(new IndexedRow(Random.nextLong(),_))

//    indexVectorRdd.collect().foreach(println(_))


    val mat: IndexedRowMatrix = new IndexedRowMatrix(indexVectorRdd)
    val m = mat.numRows()
    val n = mat.numCols()
//    println(m,n)
    println(mat.rows.map(_.index).collect().foreach(println(_)))
    println(mat.rows.map(_.vector).collect().foreach(println(_)))
//    去掉 index
    val rowMat: RowMatrix = mat.toRowMatrix()
    println("===============================================")
//    println(rowMat.rows.foreach(println(_)))
//
//    println("rows------->"+rowMat.numRows())
//    println("cols------->"+rowMat.numCols())
  }

  def coordinateMatrixTest(): Unit ={
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    var vectorsRdd = MLUtils.loadVectors(sc,"data/localVectors")
    val indexVectorRdd = vectorsRdd.collect().foreach(println(_))

    val matrixEntryRDD = vectorsRdd.map(vector =>{
      vector.toArray.apply(0)
      new MatrixEntry(vector.toArray.apply(0).toLong,vector.toArray.apply(1).toLong,vector.toArray.apply(2))
    })

    val mat = new CoordinateMatrix(matrixEntryRDD)
    mat.toRowMatrix().rows.collect().foreach(println(_))
    println("------------------------------")
    println(mat.numRows())
    println(mat.numCols())
    println("------------------------------")
//    println(mat.)
//    mat.transpose()
    println("rows----->"+mat.toRowMatrix().numRows())
    println("cols----->"+mat.toRowMatrix().numCols())

//

    new MatrixEntry(1,2,3)
  }

  def blockMatrixTest(): Unit ={
    var conf = new SparkConf()
    conf.setMaster("local").setAppName("MLTest")
    var sc = new SparkContext(conf)
    var vectorsRdd = MLUtils.loadVectors(sc,"data/localVectors")
    val indexVectorRdd = vectorsRdd.collect().foreach(println(_))

    val matrixEntryRDD = vectorsRdd.map(vector =>{
      vector.toArray.apply(0)
      new MatrixEntry(vector.toArray.apply(0).toLong,vector.toArray.apply(1).toLong,vector.toArray.apply(2))
    })

    val mat = new CoordinateMatrix(matrixEntryRDD)
    val matA = mat.toBlockMatrix().cache()
    println(matA.toLocalMatrix().toString())
//    matA.validate()
    val ata = matA.transpose.multiply(matA)
    println(ata.toLocalMatrix().toString())
  }
}