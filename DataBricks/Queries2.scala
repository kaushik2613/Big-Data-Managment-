// Databricks notebook source
// MAGIC %md
// MAGIC ###KAUSHIK BUDUR

// COMMAND ----------

// MAGIC %md
// MAGIC ###MyMav ID: 1002224112

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create DataFrames from the Soccer data files

// COMMAND ----------

// Load Country table
val country_df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/FileStore/tables/Country.csv")

// Load Match Results table
val match_results_df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/FileStore/tables/Match_results.csv")

// Load Player Assists and Goals table
val player_ag_df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/FileStore/tables/Player_Assists_Goals.csv")

// Load Player Cards table
val player_cards_df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/FileStore/tables/Player_Cards.csv")

// Load Players table
val players_df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/FileStore/tables/Players.csv")

// Load World Cup History table
val worldcup_history_df = spark.read.option("header", "true").option("inferSchema", "true")
  .csv("/FileStore/tables/Worldcup_History.csv")

// Register as temporary SQL views
country_df.createOrReplaceTempView("country")
match_results_df.createOrReplaceTempView("match_results")
player_ag_df.createOrReplaceTempView("player_stats")
player_cards_df.createOrReplaceTempView("player_cards")
players_df.createOrReplaceTempView("players")
worldcup_history_df.createOrReplaceTempView("worldcup_history")

// Show available temporary views
spark.sql("SHOW TABLES").show()

println("✅ Temporary views created successfully!")


// COMMAND ----------

// MAGIC %md
// MAGIC #### 1.	Retrieve the list of country names that have won a world cup.

// COMMAND ----------

// Load Country.csv with manual schema
val country_df = spark.read.option("header", "false")
  .csv("/FileStore/tables/Country.csv")
  .toDF("country", "population", "worldcupwins", "manager", "capital")

country_df.show()

// Load Match_results.csv
val match_results_df = spark.read.option("header", "false")
  .csv("/FileStore/tables/Match_results.csv")
  .toDF("MatchID", "Date", "Time", "team1", "team2", "team1score", "team2score", "stadium_name", "host_city")

match_results_df.show()

// Load Player_Cards.csv
val player_cards_df = spark.read.option("header", "false")
  .csv("/FileStore/tables/Player_Cards.csv")
  .toDF("PlayerID", "yellow_cards", "red_cards")

player_cards_df.show()

// Load Player_Assists_Goals.csv
val player_stats_df = spark.read.option("header", "false")
  .csv("/FileStore/tables/Player_Assists_Goals.csv")
  .toDF("PlayerID", "matches_played", "goals", "assists", "minutes_played")

player_stats_df.show()

// Load Players.csv
val players_df = spark.read.option("header", "false")
  .csv("/FileStore/tables/Players.csv")
  .toDF("PlayerID", "PlayerName", "F_name", "L_name", "DOB", "country", "height", "club", "position", "captains_country", "is_captain")

players_df.show()


// COMMAND ----------

country_df.createOrReplaceTempView("Country")

// Use SQL to filter countries with worldcupwins > 0
val countriesWithWinsDF = spark.sql("""
  SELECT country 
  FROM Country 
  WHERE worldcupwins > 0
""")

// Show the result
countriesWithWinsDF.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 2.	Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.

// COMMAND ----------

// Register temp view
country_df.createOrReplaceTempView("Country")

// Use SQL to filter, select, and order by worldcupwins
val countriesWithWins = spark.sql("""
  SELECT country, worldcupwins
  FROM Country
  WHERE worldcupwins > 0
  ORDER BY worldcupwins DESC
""")

// Display the results
countriesWithWins.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 3.	List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.

// COMMAND ----------

val capitalsByPopulation = spark.sql("""
  SELECT capital, CAST(population AS INT) AS population
  FROM Country
  WHERE CAST(population AS INT) > 100
  ORDER BY population ASC
""")

capitalsByPopulation.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 4.	List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.

// COMMAND ----------

// Load MatchResults.csv with the correct schema
val matchResultsDF = spark.read.option("header", "false")
  .csv("/FileStore/tables/Match_results.csv")
  .toDF("MatchID", "Date", "Time", "team1", "team2", "team1score", "team2score", "stadium_name", "host_city")

// Registering the MatchResults DataFrame as a temp view
matchResultsDF.createOrReplaceTempView("MatchResults")

// SQL query to get stadiums where a team scored more than 4 goals
val stadiumsWithGoalsGreaterThan4 = spark.sql("""
  SELECT DISTINCT stadium_name
  FROM MatchResults
  WHERE team1score > 4 OR team2score > 4
""")

// Display the results
stadiumsWithGoalsGreaterThan4.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 5.	List the names of all the cities which have the name of the Stadium starting with “Estadio”.

// COMMAND ----------

import org.apache.spark.sql.functions._

// Register the original DataFrame as a temporary SQL view
matchResultsDF.createOrReplaceTempView("match_results")

// Using SQL to clean the "stadium_name" column and filter for stadiums starting with "Estadio"
val stadiumnamecitiesSQL = spark.sql("""
  SELECT DISTINCT stadium_name, host_city
  FROM match_results
  WHERE LOWER(REGEXP_REPLACE(stadium_name, '[^a-zA-Z0-9 ]', '')) LIKE 'estadio%'
""")

// Show the results of distinct cities with stadiums starting with "Estadio"
stadiumnamecitiesSQL.show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC #### 6.	List all stadiums and the number of matches hosted by each stadium.

// COMMAND ----------

// Register the original DataFrame as a temporary SQL view
matchResultsDF.createOrReplaceTempView("match_results")

// Use SQL to group by stadium_name and count the number of matches for each stadium
val stadiumMatchCountSQL = spark.sql("""
  SELECT stadium_name, COUNT(*) AS match_count
  FROM match_results
  GROUP BY stadium_name
  ORDER BY match_count DESC
""")

// Displaying the results with stadium names and match count
stadiumMatchCountSQL.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 7.	List the First Name, Last Name and Date of Birth of Players whose heights are greater than 198 cms.

// COMMAND ----------

// Load the Players.csv with proper schema
val playersDF = spark.read.option("header", "false")
  .csv("/FileStore/tables/Players.csv")
  .toDF("PlayerID", "PlayerName", "F_name", "L_name", "DOB", "country", "height", "club", "position", "captains_country", "is_captain")

// Register the DataFrame as a temporary SQL view
playersDF.createOrReplaceTempView("players")

// SQL query to filter players with height greater than 198 cms and select the required columns
val playersWithHeightGreaterThan198SQL = spark.sql("""
  SELECT F_name, L_name, DOB
  FROM players
  WHERE height > 198
""")

// Display the results with first name, last name, and date of birth
playersWithHeightGreaterThan198SQL.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### 8.	List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

// COMMAND ----------

// Step 1: Define DataFrames from source data (with correct paths)
val playersDF = spark.read.option("header", "false").csv("/FileStore/tables/Players.csv")
  .toDF("PlayerID", "PlayerName", "F_name", "L_name", "DOB", "country", "height", "club", "position", "captains_country", "is_captain")

val playerCardsDF = spark.read.option("header", "false").csv("/FileStore/tables/Player_Cards.csv")
  .toDF("PlayerID", "yellow_cards", "red_cards")

val playerStatsDF = spark.read.option("header", "false").csv("/FileStore/tables/Player_Assists_Goals.csv")
  .toDF("PlayerID", "matches_played", "goals", "assists", "minutes_played")

// Step 2: Register DataFrames as temporary SQL views
playersDF.createOrReplaceTempView("players")
playerCardsDF.createOrReplaceTempView("player_cards")
playerStatsDF.createOrReplaceTempView("player_stats")

// Step 3: Use SQL to filter captains and join with the cards and stats
val captainsWithCardsSQL = spark.sql("""
  SELECT p.F_name, p.L_name, p.position, 
         COALESCE(ps.goals, 0) AS goals
  FROM players p
  LEFT JOIN player_cards pc ON p.PlayerID = pc.PlayerID
  LEFT JOIN player_stats ps ON p.PlayerID = ps.PlayerID
  WHERE p.is_captain = 'TRUE'
    AND (CAST(pc.yellow_cards AS INT) > 2 OR CAST(pc.red_cards AS INT) = 1)
""")

// Step 4: Show the result
captainsWithCardsSQL.show()


// COMMAND ----------

// Register DataFrames as temporary SQL views
playersDF.createOrReplaceTempView("players")
playerCardsDF.createOrReplaceTempView("player_cards")
playerStatsDF.createOrReplaceTempView("player_stats")

// Use SQL to find captains with more than 2 yellow cards or exactly 1 red card and left join with stats
val captainsWithCardsSQL = spark.sql("""
  SELECT p.F_name, p.L_name, p.position, 
         COALESCE(ps.goals, 0) AS goals
  FROM players p
  LEFT JOIN player_cards pc ON p.PlayerID = pc.PlayerID
  LEFT JOIN player_stats ps ON p.PlayerID = ps.PlayerID
  WHERE p.is_captain = 'TRUE'
    AND (CAST(pc.yellow_cards AS INT) > 2 OR CAST(pc.red_cards AS INT) > 1)
""")

// Show the result
captainsWithCardsSQL.show()
