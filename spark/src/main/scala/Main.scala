import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import utils.{CoreData, MetaData}


// spark2-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
// spark-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
object Exercise extends App {

  override def main(args: Array[String]): Unit = {
    val sc = getSparkContext()

    if(args.length >= 1){
      args(0) match {
        case "1" => query1(sc)
        case "2" => query2(sc)
      }
    }
  }

  /**
   * Creates the SparkContent;
   * @return
   */
  def getSparkContext(): SparkContext = {
    // Spark 2
    val spark = SparkSession.builder.appName("BDE Spark Beshiri Vaienti").getOrCreate()
    spark.sparkContext
  }

  def query1(sc: SparkContext): Unit = {
    val rddMeta = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(MetaData.extract)
    val rddCore = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(CoreData.extract)
    val outputPathQuery1 = "movielens-output"

    val rddCoreMapped = rddCore.map(x => (x.prodID, (x.revID, x.vote)))

    val rddJoin = rddMeta
      .map(x => (x.prodID, x.brand))     //.partitionBy(p)
      .join(rddCoreMapped)

    val rddUtilityIndex = rddJoin
      .map({case (_, (brand, (revID, vote))) => ((brand, revID), vote)})
      //acc = accumulator, inizializzato con il valore specificato (0.0,0.0)
      //res1 e res2 sono i risultati parziali (quelli che in Hadoop si otterrebbero dopo la combine)
      .aggregateByKey((0.0,0.0))((acc,vote)=>(acc._1+vote,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})

    val rddUtilityIndexSorted = rddUtilityIndex
      .map({case(k,v) => (v,k)})
      .sortByKey(false)
      .saveAsTextFile(outputPathQuery1)

  }

  def query2(sc: SparkContext): Unit = {
    val rddMeta = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(MetaData.extract)
    val rddCore = sc.textFile("hdfs:/bigdata/dataset/weather-sample").map(CoreData.extract)
    val outputPathQuery2 = "movielens-output"

    val rddProductOverall = rddCore
      .map(x => (x.prodID, x.overall))
      .aggregateByKey((0.0,0.0))((acc, overall)=>(acc._1+overall,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})

    val rddBrandProducts = rddMeta.map(x => (x.brand, x.prodID)).countByKey() //BISOGNA CACHARE (FORSE

    val rddBrandWith3Products = rddMeta
      .map(x => (x.brand, x.prodID))
      .filter(x => rddBrandProducts(x._1) >= 3)

    val rddJoin = rddBrandWith3Products
      .map({case(brand, prodID) => (prodID,brand)})
      .join(rddProductOverall)
      .map({case(_, (brand, overall)) => (brand, overall)})
      .aggregateByKey((0.0,0.0))((acc, overall)=>(acc._1+overall,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})
      .reduceByKey((x, y) => {if(x < y) y else x})
      .saveAsTextFile(outputPathQuery2)

  }
  
}