import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import utils.{CoreData, MetaData}

// spark2-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
// spark-submit --class Exercise BD-302-spark-opt.jar <exerciseNumber>
object Exercise extends App {

  override def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder.appName("BDE Spark Beshiri Vaienti").getOrCreate().sparkContext

    if(args.length >= 1){
      args(0) match {
        case "1" => query1(sc)
        case "2" => query2(sc)
      }
    }
  }

  def query1(sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path("/user/avaienti/project/spark/query1"), true)

    val p = new HashPartitioner(8)

    val outputPathQuery1 = "/user/avaienti/project/spark/query1"
    val rddMeta = sc.textFile("/user/avaienti/dataset-sample/industry_meta.csv")
      .filter(x => MetaData.metaParsable(x))
      .map(x => MetaData.extract(x))
    val rddCore = sc.textFile("/user/avaienti/dataset-sample/industry_core.csv")
      .filter(x => CoreData.coreParsable(x))
      .map(x => CoreData.extract(x))

    val rddCoreMapped = rddCore.map(x => (x.prodID, (x.revID, x.vote))).partitionBy(p)

    val rddJoin = rddMeta
      .map(x => (x.prodID, x.brand))
      .partitionBy(p)
      .join(rddCoreMapped)

    val rddUtilityIndex = rddJoin
      .map({case (_, (brand, (revID, vote))) => ((brand, revID), vote)})
      .aggregateByKey((0.0,0.0))((acc,vote)=>(acc._1+vote,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})
    //acc = accumulator, inizializzato con il valore specificato (0.0,0.0)
    //res1 e res2 sono i risultati parziali (quelli che in Hadoop si otterrebbero dopo la combine)

    val rddUtilityIndexSorted = rddUtilityIndex
      .map({case((brand, revID), utilityIndex) => ((brand, BigDecimal(utilityIndex).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble), revID)})
      .groupByKey()
      .sortByKey()
      .saveAsTextFile(outputPathQuery1)

  }

  def query2(sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path("/user/avaienti/project/spark/query2"), true)

    val p = new HashPartitioner(8)

    val outputPathQuery2 = "/user/avaienti/project/spark/query2"
    val rddMetaCached = sc.textFile("/user/avaienti/dataset-sample/industry_meta.csv")
      .filter(x => MetaData.metaParsable(x))
      .map(x => MetaData.extract(x))
      .map(x => (x.brand, x.prodID))
      .cache()
    val rddCore = sc.textFile("/user/avaienti/dataset-sample/industry_core.csv")
      .filter(x => CoreData.coreParsable(x))
      .map(x => CoreData.extract(x))

    val rddProductOverall = rddCore
      .map(x => (x.prodID, x.overall))
      .aggregateByKey((0.0,0.0))((acc, overall)=>(acc._1+overall,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})
      .partitionBy(p)

    val broadcastRddBrandProducts = sc.broadcast(rddMetaCached.countByKey()) //VARIABILE CONDIVISA

    val rddBrandWith3Products = rddMetaCached
      .filter(x => broadcastRddBrandProducts.value(x._1) >= 2)

    val rddJoin = rddBrandWith3Products
      .map({case(brand, prodID) => (prodID,brand)})
      .partitionBy(p)
      .join(rddProductOverall)
      .map({case(_, (brand, overall)) => (brand, overall)})
      .aggregateByKey((0.0,0.0))((acc, overall)=>(acc._1+overall,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2))
      .map({case(k,v)=>(k, v._1/v._2)})
      .cache()

    val maxOverall = rddJoin.values.max

    val rddBrandsWithHigherOverall = rddJoin
      .filter{ case (_, v) => v == maxOverall }
      .map({case(brand, overall) => (overall,brand)})
      .groupByKey()
      .saveAsTextFile(outputPathQuery2)

  }

}