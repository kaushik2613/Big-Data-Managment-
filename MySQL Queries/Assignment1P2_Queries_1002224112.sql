

-- Assignment 1 Phase 2
-- Queries: 

-- 1) Retrieve the list of countries that have won a world cup
USE SOCCERA2;
SHOW TABLES;
DESCRIBE COUNTRY;
SELECT CountryName
FROM COUNTRY
WHERE no_of_world_cups_won > 0;


-- 2) Retrieve the list of country names that have won a world cup and the number of  world cups each has won in descending order.
SELECT CountryName, no_of_world_cups_won
FROM COUNTRY
WHERE no_of_world_cups_won > 0
ORDER BY no_of_world_cups_won DESC;


-- 3) List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.
SELECT capital, countryname, population
FROM COUNTRY
WHERE population > 100
ORDER BY population ASC;


-- 4) List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.
SHOW TABLES;
DESCRIBE MATCH_RESULTS;
SELECT DISTINCT stadium
FROM MATCH_RESULTS
WHERE score1 > 4 OR score2 > 4;


-- 5) List the names of all the cities which have the name of the Stadium starting with “Estadio”.
SELECT DISTINCT city
FROM MATCH_RESULTS
WHERE stadium LIKE 'Estadio%';



-- 6) List all stadiums and the number of matches hosted by each stadium. 
SELECT stadium, COUNT(*) AS num_matches
FROM MATCH_RESULTS
GROUP BY stadium;



-- 7) List the First Name, Last Name and Date of Birth of Players whose heights is greater than 198 cms
SHOW TABLES;
DESCRIBE PLAYERS;
SELECT Fname, Lname, BirthDate
FROM Players
WHERE Height > 198;

--   Challenge (bonus) Questions :

-- 8) List the Stadium Names and the Teams (Team1 and Team2) that played Matches   between 20-Jun-2014 and 24-Jun-2014
SELECT stadium, team1, team2
FROM MATCH_RESULTS
WHERE match_date BETWEEN '2014-06-20' AND '2014-06-24';


-- 9) List the Fname, Lname, Position and No of Goals scored by the Captain of a team  who has more than 2 Yellow cards or 1 Red card . 
SELECT p.Fname, p.Lname, p.Position, COALESCE(g.Goals, 0) AS Goals
FROM Players p
JOIN Player_Cards c ON p.PID = c.PID
LEFT JOIN Player_Assists_Goals g ON p.PID = g.PID
WHERE p.isCaptain = 'TRUE'  -- Now comparing to the text value 'TRUE'
  AND (c.no_of_Yellow_Cards > 2 OR c.no_of_Red_Cards >= 1);







