import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.feature.StandardScaler
import breeze.linalg.DenseMatrix
import breeze.linalg.csvwrite

import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.io.File

object pca{
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("pca")
    val sc = new SparkContext(conf)

    val path = "lfw/*"
    val rdd = sc.wholeTextFiles(path)
    val first = rdd.first
    //println(first)

    val files = rdd.map{case (fileName, content) => fileName.replace("file:", "")}
    val zz = files.first
    println(files.count)

    def loadImageFromFile(path: String):BufferedImage = {
      import javax.imageio.ImageIO
      import java.io.File
      ImageIO.read(new File(path))
    }
    val aePath = zz
    val aeImage = loadImageFromFile(aePath)

    def processImage(image: BufferedImage, width: Int, height: Int):BufferedImage = {
      val bwImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
      val g = bwImage.getGraphics()
      g.drawImage(image, 0, 0, width, height, null)
      g.dispose()
      bwImage
    }

    val grayImage = processImage(aeImage, 100, 100)
    ImageIO.write(grayImage, "jpg", new File("./aeGray.jpg"))

    def getPixelsFromImage(image: BufferedImage): Array[Double] = {
      val width = image.getWidth
      val height = image.getHeight
      val pixels = Array.ofDim[Double](width * height)
      image.getData.getPixels(0, 0, width, height, pixels)
    }

    def extractPixels(path: String, width: Int, height: Int): Array[Double] = {
      val raw = loadImageFromFile(path)
      val processed = processImage(raw, width, height)
      getPixelsFromImage(processed)
    }

    val pixels = files.map(f => extractPixels(f, 50, 50))
    println(pixels.take(10).map(_.take(10).mkString("", ",", ", ...")).mkString("\n"))
    val vectors = pixels.map(p => Vectors.dense(p))
    vectors.setName("image-vectors")
    vectors.cache

    val scaler = new StandardScaler(withMean = true, withStd = false).fit(vectors)
    val scaledVectors = vectors.map(v => scaler.transform(v))
    
    val matrix = new RowMatrix(scaledVectors)
    val K = 10
    val pc = matrix.computePrincipalComponents(K)
    val rows = pc.numRows
    val cols = pc.numCols
    println(rows, cols)

    val pcBreeze = new DenseMatrix(rows, cols, pc.toArray)
    csvwrite(new File("./pc.csv"), pcBreeze)

    val projected = matrix.mutiply(pc)
    println(projected.numRows, projected.numCols)
    println(projected.rows.take(5).mkString("\n"))

    sc.stop()
}
}
