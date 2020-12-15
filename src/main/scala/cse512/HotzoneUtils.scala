package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    if (queryRectangle == null || queryRectangle.isEmpty())
      return false
    else if (pointString == null || pointString.isEmpty())
      return false

    val rectangle = queryRectangle.split(",")
    val x1 = rectangle(0).toDouble
    val y1 = rectangle(1).toDouble
    val x2 = rectangle(2).toDouble
    val y2 = rectangle(3).toDouble

    val point = pointString.split(",")
    val x = point(0).toDouble
    val y = point(1).toDouble

    if (x >= x1 && x <= x2 && y >= y1 && y <= y2)
      return true
    else if (x >= x2 && x <= x1 && y >= y2 && y <= y1)
      return true
    else
      return false
  }
}
