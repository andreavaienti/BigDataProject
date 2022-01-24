import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner
import utils.{CoreData, MetaData}

// spark2-submit --class Exercise --num-executors X --executor-cores Y --executor-memory Zg BDE-spark-Beshiri-Vaienti.jar QueryNumber User PartitionNumber
object Exercise extends App {

  override def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder.appName("BDE Spark Beshiri Vaienti").getOrCreate().sparkContext

    if(args.length >= 3){
      (args(0), args(1)) match {
        case ("1", "andrea") => query1(sc, "avaienti", args(2).toInt)
        case ("1", "rei") => query1(sc, "brei", args(2).toInt)
        case ("2", "andrea") => query2(sc, "avaienti", args(2).toInt)
        case ("2", "rei") => query2(sc, "brei", args(2).toInt)
        case _ => System.out.println("Parameters required: <Query Number> <Output Dir> [OPTIONAL <num partitions>]")
      }
    } else {
      System.out.println("Parameters required: <Query Number> <Output Dir> [OPTIONAL <num partitions>]")
    }
  }

  def query1(sc: SparkContext, user: String, nPartitions: Int): Unit = {

    val p = new HashPartitioner(nPartitions)

    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path("/user/"+user+"/project/spark/query1"), true)

    val outputPathQuery1 = "/user/"+user+"/project/spark/query1"

    val rddMeta = sc.textFile("/user/avaienti/dataset/meta.csv", nPartitions)
      .filter(x => MetaData.metaParsable(x))
      .map(x => MetaData.extract(x))
      .map(x => (x.prodID, x.brand))
      .partitionBy(p)

    val rddCore = sc.textFile("/user/avaienti/dataset/core.csv", nPartitions)
      .filter(x => CoreData.coreParsable(x))
      .map(x => CoreData.extract(x))
      .map(x => (x.prodID, (x.revID, x.vote)))
      .partitionBy(p)

    val rddJoin = rddMeta.join(rddCore)

    val rddUtilityIndex = rddJoin
      .map({case (_, (brand, (revID, vote))) => ((brand, revID), vote)})
      .aggregateByKey((0.0,0.0))((acc,vote)=>(acc._1+vote,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})

    //rddUtilityIndexSorted
    rddUtilityIndex
      .map({case((brand, revID), utilityIndex) => ((brand, BigDecimal(utilityIndex).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble), revID)})
      .groupByKey()
      .sortByKey()
      .saveAsTextFile(outputPathQuery1)

  }

  def query2(sc: SparkContext, user: String, nPartitions: Int): Unit = {

    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path("/user/"+user+"/project/spark/query2"), true)

    val p = new HashPartitioner(nPartitions)

    val outputPathQuery2 = "/user/"+user+"/project/spark/query2"

    val rddMetaCached = sc.textFile("/user/avaienti/dataset/meta.csv", nPartitions)
      .filter(x => MetaData.metaParsable(x))
      .map(x => MetaData.extract(x))
      .map(x => (x.brand, x.prodID))
      .cache()

    val rddCore = sc.textFile("/user/avaienti/dataset/core.csv", nPartitions)
      .filter(x => CoreData.coreParsable(x))
      .map(x => CoreData.extract(x))
      .map(x => (x.prodID, x.overall))

    val rddProductOverall = rddCore
      .aggregateByKey((0.0,0.0))((acc, overall)=>(acc._1+overall,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2)).map({case(k,v)=>(k,v._1/v._2)})
      .partitionBy(p)

    val broadcastDictBrandProductsCounter = sc.broadcast(rddMetaCached.countByKey()) //VARIABILE CONDIVISA

    val rddBrandWith3Products = rddMetaCached
      .filter(x => broadcastDictBrandProductsCounter.value(x._1) >= 2)

    val rddJoin = rddBrandWith3Products
      .map({case(brand, prodID) => (prodID,brand)})
      .partitionBy(p)
      .join(rddProductOverall)

    val rddBrandOverall = rddJoin
      .map({case(_, (brand, overall)) => (brand, overall)})
      .aggregateByKey((0.0,0.0))((acc, overall)=>(acc._1+overall,acc._2+1), (res1, res2)=>(res1._1+res2._1,res1._2+res2._2))
      .map({case(k,v)=>(k, v._1/v._2)})
      .cache()

    val broadcastMaxOverall = sc.broadcast(rddBrandOverall.values.max) //VARIABILE CONDIVISA

    //rddBrandsWithHigherOverall
    rddBrandOverall
      .filter{ case (_, v) => v == broadcastMaxOverall }
      .coalesce(1)
      .map({case(brand, overall) => (overall,brand)})
      .groupByKey()
      .saveAsTextFile(outputPathQuery2)

  }

}