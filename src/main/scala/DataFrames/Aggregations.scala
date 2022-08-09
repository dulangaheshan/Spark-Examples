package DataFrames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark = SparkSession.builder().appName("Aggregation and Grouping").config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

  // counting

  val genresCount = moviesDF.select(count("Major_Genre")) // not include null

  moviesDF.selectExpr("count(Major_Genre)").show()

  moviesDF.select(count("*")).show() // include nulls

  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max

  moviesDF.select(min(col("IMDB_Rating"))).show()

  moviesDF.selectExpr("min(IMDB_Rating)").show()

  moviesDF.select(sum(col("US_Gross"))).show()

  moviesDF.selectExpr("sum(US_Gross)").show()

  moviesDF.select(avg("Rotten_Tomatoes_Rating")).show()

  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)").show()

  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()


  // Grouping

  val countByGenreDF = moviesDF             // select count(*) from moviesDF group by Major_Genre
    .groupBy(col("Major_Genre")) // include null also
    .count()

  countByGenreDF.show()

  val avgRatingByGenreDF = moviesDF            // select avg("IMDB_Rating") from moviesDF group by Major_Genre;
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  avgRatingByGenreDF.show()

  val aggregationByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy("Avg_Rating")

  aggregationByGenreDF.show()


  /**
   * Exercises
   *

   *

   */

  //  * 1. Sum up ALL the profits of ALL the movies in the DF

  val totalSumDF = moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total"))
    .select(sum("Total")).show()

// 2. Count how many distinct directors we have

  val distinctDirectorsDF = moviesDF.select(countDistinct(col("Director")))
  distinctDirectorsDF.show()

//  3. Show the mean and standard deviation of US gross revenue for the movies

  val stdAndMeanOfMoviesDF = moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  )

  stdAndMeanOfMoviesDF.show()

//  * 4. Compute the average IMDB rating and the Total US gross revenue PER DIRECTOR

  val avgIMDBRatingAndUSGross = moviesDF.groupBy("Director")
    .agg(
      avg(col("IMDB_Rating")).as("AVG_Rating"),
      sum(col("US_Gross")).as("Total_gross")
    ).orderBy(col("Avg_rating").desc_nulls_last)         // desc_nulls_last


  avgIMDBRatingAndUSGross.show()



}
