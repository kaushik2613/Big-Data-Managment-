-- Select columns related to the match details from the match_results table
SELECT 
    match_date,          -- The date when the match took place
    team1,               -- The first team playing in the match
    team2,               -- The second team playing in the match
    score1,              -- The score of team1 in the match
    score2,              -- The score of team2 in the match
    stadium,             -- The stadium where the match was held
    city                 -- The city where the match was held
FROM match_results;     -- The table that contains the match results
