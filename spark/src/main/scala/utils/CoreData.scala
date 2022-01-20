package utils

object CoreData {
  def extract(row:String) = {

    def getDouble(str:String) : Double = if(str.isEmpty) 0 else str.toDouble

    val columns = row.split(",")
    CoreData(getDouble(columns(0)), columns(1), columns(2), getDouble(columns(3)))
  }

  def coreParsable(x: String): Boolean = {
    val split = x.split(",")
    def isAllDigits(x: String) = x forall Character.isDigit
    split.length == 4 &&
      split(0).length > 0 &&
      isAllDigits(split(0)) &&
      split(1).length > 0  &&
      split(2).length > 0 &&
      split(3).length > 0 &&
      isAllDigits(split(3))
  }
}

case class CoreData(
   overall: Double,
   revID: String,
   prodID: String,
   vote: Double
)

