-- Use the 'soccera2' database
USE soccera2;

-- Drop the 'player_statistics' table if it already exists to avoid errors
DROP TABLE IF EXISTS player_statistics;

-- Create the 'player_statistics' table by joining relevant data from multiple tables
CREATE TABLE player_statistics AS
SELECT 
    -- Select country details from the 'country' table
    c.CountryName, 
    c.capital, 
    c.population, 
    c.coach,

    -- Select player details from the 'players' table
    p.Lname AS Player_Lname,
    p.Fname AS Player_Fname,
    p.Height,
    p.BirthDate,
    p.isCaptain,
    p.Position,

    -- Use COALESCE to handle null values in the 'player_cards' table for yellow and red cards
    COALESCE(pc.no_of_yellow_cards, 0) AS no_Yellow_cards, 
    COALESCE(pc.no_of_red_cards, 0) AS no_Red_cards, 

    -- Use COALESCE to handle null values in the 'player_assists_goals' table for goals and assists
    COALESCE(pag.goals, 0) AS no_Goals,
    COALESCE(pag.assists, 0) AS no_Assists
FROM 
    -- The 'country' table contains country details
    country c

-- Join the 'players' table on the 'Country' field to match players with their respective countries
LEFT JOIN 
    players p ON p.Country = c.CountryName

-- Join the 'player_cards' table on the 'PID' (Player ID) to get the player's card details
LEFT JOIN 
    player_cards pc ON pc.PID = p.PID

-- Join the 'player_assists_goals' table on the 'PID' to get the player's goals and assists data
LEFT JOIN 
    player_assists_goals pag ON pag.PID = p.PID;

-- Query the newly created 'player_statistics' table to view the results
SELECT * FROM player_statistics;
