package utils

object MetaData {
  def extract(row:String) = {
    val columns = row.split(",")
    MetaData(columns(0),columns(1))
  }

  def metaParsable(x: String): Boolean = {
    val split = x.split(",")
    split.length == 2 && split(0).length > 0 && split(1).length > 0
  }

}

case class MetaData(
   brand:String,
   prodID:String
)

/*object MetaData2 {
  def extract(brand:String, prodID: String) = {
    MetaData2(brand,prodID)
  }
}

case class MetaData2 (
  brand:String,
  prodID:String
)*/
