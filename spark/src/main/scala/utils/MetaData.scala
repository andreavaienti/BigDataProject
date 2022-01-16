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
