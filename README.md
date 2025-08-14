# CSE512-Hotspot-Analysis

## Introduction

This project provides a framework for performing hotspot and hotzone analysis on spatial data, using NYC taxi trip data as an example. It leverages Apache Spark to efficiently process large datasets and identify areas with high concentrations of events.

The project is divided into two main analysis modules:

*   **Hotcell Analysis**: Identifies "hot cells" in a 3D grid of space and time. This is useful for finding clusters of events that are close to each other both spatially and temporally.
*   **Hotzone Analysis**: Determines which predefined zones (rectangles) are "hot" by counting the number of events that fall within each zone.

## Technologies

*   **Scala**: The primary programming language used for the project.
*   **Apache Spark**: A distributed computing system used for large-scale data processing.
*   **SBT**: The build tool for the project.

## Modules

### Hotcell Analysis

The Hotcell Analysis module (`HotcellAnalysis.scala`) performs the following steps:

1.  **Load Data**: Reads NYC taxi pickup data from a CSV file.
2.  **Discretize Coordinates**: Converts the latitude, longitude, and time of each pickup into a discrete 3D grid cell (x, y, z).
3.  **Filter Data**: Removes data points that fall outside a predefined area of interest.
4.  **Count Pickups per Cell**: Calculates the number of pickups in each cell.
5.  **Calculate G-Score**: Computes the Getis-Ord G-score for each cell. The G-score is a measure of spatial clustering that indicates how intense the clustering of events is in a given area.
6.  **Identify Hotspots**: The top 50 cells with the highest G-scores are identified as hotspots.

### Hotzone Analysis

The Hotzone Analysis module (`HotzoneAnalysis.scala`) performs a simpler analysis:

1.  **Load Data**: Loads two datasets: one containing points of interest and another containing predefined rectangular zones.
2.  **Spatial Join**: Counts the number of points that fall within each rectangle.
3.  **Identify Hotzones**: The output is a list of rectangles and the number of points they contain, which can be used to identify hotzones.

## How to Run

1.  **Compile the project:**
    ```bash
    sbt package
    ```

2.  **Run the analysis:**
    Use `spark-submit` to run the desired analysis. The main class is `cse512.Entrance`.

    **Hotcell Analysis:**
    ```bash
    spark-submit --class cse512.Entrance target/scala-2.11/cse512-hotspot-analysis-template_2.11-0.1.0.jar <output_path> hotcellanalysis <input_data_path>
    ```

    **Hotzone Analysis:**
    ```bash
    spark-submit --class cse512.Entrance target/scala-2.11/cse512-hotspot-analysis-template_2.11-0.1.0.jar <output_path> hotzoneanalysis <point_data_path> <rectangle_data_path>
    ```

## Input Data

*   **Hotcell Analysis**: Expects a CSV file with pickup data. The relevant columns are pickup time and coordinates.
*   **Hotzone Analysis**: Expects two files:
    *   A CSV file with point data.
    *   A file with rectangle data, where each line defines a rectangle.

## Output

The output of both analyses is a CSV file containing the results:
*   **Hotcell Analysis**: A list of the top 50 hot cells (x, y, z coordinates).
*   **Hotzone Analysis**: A list of rectangles and the number of points they contain.
