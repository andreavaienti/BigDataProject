package utils

object MetaData {
  def extract(row:String) = {
    val columns = row.split(",")
    MetaData(columns(0),columns(1))
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
