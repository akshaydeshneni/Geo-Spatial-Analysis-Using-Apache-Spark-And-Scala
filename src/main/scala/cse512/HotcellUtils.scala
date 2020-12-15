package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.math
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._


object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def checkAdjacency(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int): Boolean = {

    if((x2 == x1+1 || x2 == x1 || x2 == x1-1) && (y2 == y1+1 || y2 == y1 || y2 == y1-1) && (z2 == z1+1 || z2 == z1 || z2 == z1-1))
      return true
    else
      return false
  }

  val isAdjacent: UserDefinedFunction = udf[Boolean, Int, Int, Int, Int, Int, Int](checkAdjacency)

  def checkNeighbors(minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int, maxZ: Int, X: Int, Y: Int, Z: Int): Int = {

    val cubeLocation: Map[Int, String] = Map(0->"inside", 1->"face", 2->"edge", 3->"corner")
    val neighbors: Map[String, Int] = Map("inside"->27, "face"->18, "edge"->12, "corner"->8)

    var count = 0

    if(X == minX || X == maxX)
      count += 1
    if(Y == minY || Y == maxY)
      count += 1
    if(Z == minZ || Z == maxZ)
      count += 1

    var location = cubeLocation.get(count).get.toString()

    return neighbors.get(location).get.toInt
  }

  def checkNeighborsUDF: UserDefinedFunction = udf[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int](checkNeighbors)

  def gScore(average: Double, standard_deviation: Double, numCells: Double) (sum: Long, adjacentCount: Long): Double =
  {
    val adjacentCountDouble = adjacentCount.toDouble
    val sumDouble = sum.toDouble
    val value = (sumDouble - (average * adjacentCountDouble)) / (standard_deviation * math.sqrt((numCells * adjacentCountDouble - (adjacentCountDouble * adjacentCountDouble)) / (numCells - 1.0)))
    return value
  }

  def g_score(average: Double, standard_deviation: Double, numCells: Double): UserDefinedFunction = udf[Double, Long, Long](gScore(average, standard_deviation, numCells))
}
