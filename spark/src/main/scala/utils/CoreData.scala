package utils

object CoreData {
  def extract(row:String) = {

    def getDouble(str:String) : Double = if(str.isEmpty) 0 else str.toDouble

    val columns = row.split(",")
    CoreData(getDouble(columns(0)), columns(1), columns(2), getDouble(columns(4)))
  }
}

case class CoreData(
   overall: Double,
   revID: String,
   prodID: String,
   vote: Double
)
