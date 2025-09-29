// Databricks notebook source
// MAGIC %md
// MAGIC ##KAUSHIK BUDUR

// COMMAND ----------

// MAGIC %md
// MAGIC ###MyMAvID: 1002224112

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create DataFrames from the Soccer data files

// COMMAND ----------

dbutils.fs.ls("/FileStore/tables/").foreach(println)


// COMMAND ----------

// Load Country table
val country_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Country.csv")

// Load Match Results table
val match_results_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Match_results.csv")

// Load Player Assists and Goals table
val player_ag_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Player_Assists_Goals.csv")

// Load Player Cards table
val player_cards_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Player_Cards.csv")

// Load Players table
val players_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Players.csv")

// Load World Cup History table
val worldcup_history_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/tables/Worldcup_History.csv")


// COMMAND ----------

spark.catalog.listTables().show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.	Retrieve the list of country names that have won a world cup.

// COMMAND ----------

// Select distinct country names that have won a World Cup
val worldcup_winners_df = worldcup_history_df
  .select("Winner")
  .distinct()
  .filter("Winner IS NOT NULL")

worldcup_winners_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.	Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.

// COMMAND ----------

// Group by the Winner (country) and count the number of World Cups each country has won
val worldcup_count_df = worldcup_history_df
  .groupBy("Winner")
  .count()
  .filter("Winner IS NOT NULL")  // Remove any null entries
  .orderBy($"count".desc)  // Order by count in descending order

worldcup_count_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.	List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.

// COMMAND ----------

// Filter countries with population greater than 100 million, select country, capital, and population
val countries_with_high_population_df = country_df
  .filter($"Population" > 100)  // Filter for countries with population > 100 million
  .select("CountryName", "Capital", "Population")  // Select country name, capital, and population
  .orderBy($"Population".asc)  // Order by population in increasing order

countries_with_high_population_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.	List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.

// COMMAND ----------

// Filter matches where a single team's goals are greater than 4
val stadiums_df = match_results_df
  .filter($"Score1" > 4 || $"Score2" > 4)  // Filter for teams scoring more than 4 goals
  .select("Stadium")  // Select the stadium name
  .distinct()  // Remove duplicates (if the same stadium hosted multiple such matches)

stadiums_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.	List the names of all the cities which have the name of the Stadium starting with “Estadio”.

// COMMAND ----------

// Filter matches where the Stadium name starts with "Estadio"
val estadio_cities_df = match_results_df
  .filter($"Stadium".startsWith("Estadio"))  // Filter for stadium names starting with "Estadio"
  .select("City")  // Select the City column
  .distinct()  // Remove duplicates

// Display the list of cities
estadio_cities_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.	List all stadiums and the number of matches hosted by each stadium.

// COMMAND ----------

// Group by Stadium and count the number of matches hosted by each stadium
val stadium_match_count_df = match_results_df
  .groupBy("Stadium")  // Group by the Stadium name
  .count()  // Count the number of matches hosted by each stadium
  .orderBy($"count".desc)  // Order the results by the match count in descending order

stadium_match_count_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 7.	List the First Name, Last Name and Date of Birth of Players whose heights are greater than 198 cms.

// COMMAND ----------

// Filter players with height greater than 198 cm
val tall_players_df = players_df
  .filter($"Height" > 198)  // Filter for players with height greater than 198 cm
  .select("FName", "LName", "BirthDate")  // Select the required columns

tall_players_df.show(false)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 8.	List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

// COMMAND ----------

// Step 1: Join all relevant DataFrames on "PID" (Player ID)
val playerStats = players_df
  .join(player_cards_df, "PID")   // Include yellow/red card data
  .join(player_ag_df, "PID")      // Include performance stats (goals, etc.)

// Step 2: Filter to find captains with more than 2 yellow cards OR exactly 1 red card
val filteredCaptains = playerStats.filter(
  $"isCaptain" === true &&                        // Only captains
  (
    ($"no_of_yellow_cards" > 2)                   // More than 2 yellow cards
    .or($"no_of_red_cards" === 1)                 // OR exactly 1 red card
  )
)

// Step 3: Select the required columns
filteredCaptains
  .select("Fname", "Lname", "Position", "goals")  // Output: First name, Last name, Position, Goals
  .show(false)                                    // Show full column values without truncation


// COMMAND ----------

// MAGIC %md
// MAGIC %md
// MAGIC #### 8.	List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

// COMMAND ----------

import org.apache.spark.sql.functions._ // Make sure to import this

// Step 1: Join players_df with player_cards_df (inner join, card info is required)
val playerWithCards = players_df
  .join(player_cards_df, Seq("PID"), "inner") // Only keep players who have card records

// Step 2: Join with player_ag_df using left join (some players might not have goal/assist data)
val playerStats = playerWithCards
  .join(player_ag_df, Seq("PID"), "left_outer") // Keep all players even if they have no goal/assist stats

// Step 3: Filter captains who have more than 2 yellow cards OR exactly 1 red card
val filteredCaptains = playerStats.filter(
  $"isCaptain" === true &&                                   // Must be a captain
  (
    ($"no_of_yellow_cards" > 2)                              // More than 2 yellow cards
    .or(coalesce($"no_of_red_cards".cast("int"), lit(0)) === 1)  // OR exactly 1 red card (handle nulls safely)
  )
)

// Step 4: Select relevant output columns (first name, last name, position, goals)
filteredCaptains
  .select($"Fname", $"Lname", $"Position", 
          coalesce($"goals", lit(0)).alias("goals")) // Replace null goals with 0
  .show(false)  // Show full column values without truncation
