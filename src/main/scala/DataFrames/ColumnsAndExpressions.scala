package DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, exp, expr}

object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF")
    .config("spark.master", "local")
    .getOrCreate()

  val carDF = spark.read
    .option("inferSchema", "true")
    .json("/home/d5han/Documents/projects/spark_examples/src/main/resources/data/cars.json")

  carDF.show()

carDF.columns.foreach(u => {
    println(u)
  } )


  val firstColumn = carDF.col("Name")

  // selection -> projecting
  val carNameDF = carDF.select(firstColumn)
  carNameDF.show()

  // various select methods
  import spark.implicits._
  carDF.select(
    carDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carDF.select("Name", "Year")

  // Expressions

  val simpleExpression = carDF.col("weight_in_lbs")
  val weightINKgExpression = carDF.col("weight_in_lbs") / 2.2

  val carsWithWeightsDf = carDF.select(
    col("Name"),
    col("weight_in_lbs"),
    weightINKgExpression.as("weight_in_kg")
  )

  val carsWithWeightsDF = carDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightINKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightsDF = carDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  carsWithSelectExprWeightsDF.show(20)

  // DF processing

  val carsWithKG3DF = carDF.withColumn("weight_in_kg3", col("weight_in_lbs")/2.2)

  carsWithKG3DF.show()

  val carsWithColumnsRenamed = carDF.withColumnRenamed("weight_in_lbs", "weight in pounds")

  carsWithColumnsRenamed.selectExpr("`weight in pounds`").show()

  // remove a column
  carsWithColumnsRenamed.drop("Cylinders", "Displacement")

  // filtering and where does same thing

  val europeanCarsDF = carDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carDF.where(col("Origin") =!= "USA")
  europeanCarsDF.show()

  //also with expression strings

  val americanCarsDF = carDF.filter("Origin = 'USA'")

  // chain filters

  // chain filters
  val americanPowerfulCarsDF = carDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carDF.filter((col("Origin") === "USA") and (col("Horsepower") > 150))

  val americanPowerfulCarsDF3 = carDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carDF.select("Origin").distinct()


  /**
   * Exercises
   *
   * 1. Read the movies DF and select 2 columns of your choice
   * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
   * 3. Select all COMEDY movies with IMDB rating above 6
   *
   * Use as many versions as possible
   */

  val moviesDF =  spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  moviesDF.show(10)

  val selectedCols = moviesDF.select(
    col("Creative_Type"),
    col("Director")

  )

  selectedCols.show()

  val moviesWithTotalProfitDF = moviesDF.withColumn("total_profit",(col("US_Gross") + col("Worldwide_Gross")))

  moviesWithTotalProfitDF.show(20)

  val filteredComedyMovies = moviesDF.filter("Major_Genre == 'Drama' and IMDB_Rating > 6")

  println(filteredComedyMovies.count())



}
