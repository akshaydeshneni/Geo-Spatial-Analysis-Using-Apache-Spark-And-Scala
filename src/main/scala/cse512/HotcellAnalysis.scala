package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import scala.math

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    //pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // Definition of String Variables which will be used as column names throughout the rest of the code

    val X = "x"
    val Y = "y"
    val Z = "z"
    val Count = "count"
    val Sum = "sum"
    val NumAdjacent = "num_of_adj_cells"
    val GScore = "g_score"
    val DF1 = "df1"
    val DF2 = "df2"
    val Square = "squared_count"

    // Selection Criteria - X, Y and Z should be between there minimum and maximum values (including min and max). We store the modified output in pickUpInfo

    pickupInfo = pickupInfo.select(X, Y, Z).where("x >= " + minX + " AND x <= " + maxX + " AND y >= " + minY + " AND y <= " + maxY + " AND z >= " + minZ + " AND z <= " + maxZ)

    //pickupInfo.show()

    // pickUpInfo DataFrame is then grouped by X, Y, Z and then cached for faster retrieval and stored as hotCellDataFrame

    var hotCellDataFrame = pickupInfo.groupBy(X, Y, Z).count().cache()

    // Calculating the Average

    val average = (hotCellDataFrame.select(Count).agg(sum(Count)).first().getLong(0).toDouble) / numCells

    // Calculating the Standard Deviation

    val standard_deviation = math.sqrt((hotCellDataFrame.withColumn(Square, pow(col(Count), 2)).select(Square).agg(sum(Square)).first().getDouble(0) / numCells) - math.pow(average, 2))

    // We created two views on the hotCellDataFrame and filtered the records which are adjacent to each other. The implementation of isAdjacent UDF can be found in HotcellUtils.scala
    // After the filtering the records, we have selected only the X, Y, Z from DF1 and Count from DF2
    // Then we have grouped by X, Y, Z and aggregated the sum(Count) as New column Sum

    var adjacencyDataFrame = hotCellDataFrame.as(DF1).join(hotCellDataFrame.as(DF2))
      .filter(HotcellUtils.isAdjacent(col(s"$DF1.$X"), col(s"$DF1.$Y"), col(s"$DF1.$Z"),
        col(s"$DF2.$X"), col(s"$DF2.$Y"), col(s"$DF2.$Z")))
      adjacencyDataFrame = adjacencyDataFrame.select(col(s"$DF1.$X"), col(s"$DF1.$Y"), col(s"$DF1.$Z"), col(s"$DF2.$Count"))
      adjacencyDataFrame = adjacencyDataFrame.groupBy(X, Y, Z).agg(sum(Count) as Sum)

    //adjacencyDataFrame.show()

    // Checking the number of neighbors and adding a new column num_of_adj_cells to the adjacencyFinalDataFrame

    val adjacencyFinalDataFrame = adjacencyDataFrame.withColumn(NumAdjacent, HotcellUtils.checkNeighborsUDF(lit(minX), lit(minY), lit(minZ), lit(maxX), lit(maxY), lit(maxZ), col(X), col(Y), col(Z)))

    //adjacencyFinalDataFrame.show()

    // Calculating the G-score and assigning it to the finalDataFrame and limiting the number of records to 50

    var finalDataFrame = adjacencyFinalDataFrame.withColumn(GScore, HotcellUtils.g_score(average, standard_deviation, numCells) (col(Sum), col(NumAdjacent)))
    finalDataFrame = finalDataFrame.sort(desc(GScore)).limit(50)

    // Finally updating the pickUpInfo DataFrame with finalDataFrame

    pickupInfo = finalDataFrame.select(col(X), col(Y), col(Z))
    //pickupInfo.show()

    return pickupInfo // YOU NEED TO CHANGE THIS PART
  }
}
