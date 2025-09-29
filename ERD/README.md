Create a conceptual schema database design using the ER/EER model, then convert it to a schema using the rules for ER/EER to relational


The database that you will design in project 1 will keep track of the FIFA 2014 Soccer World Cup teams, match results, players, among other information. 


Here are the requirements for the database:


1. We want to store and query information for the SOCCER database for the tournament known as the FIFA (the acronym for the world soccer governing body) 2014
men's World Cup, which is held once every 4 years. FIFA is divided into 6 confederations divided by continents: AFC (Asia), CAF (Africa), CONCACAF (North and
Central America and the Caribbean), CONMEBOL (South America), OFC (Oceania - New Zealand and Pacific Island Countries), and UEFA (Europe). Most world
countries are members of FIFA through one of its confederations, and the countries in each confederation play qualifying tournaments to determine which
countries will participate in the world cup tournament, held every 4 years.



2. In 2014, 32 countries participated in the world cup tournament, held in Russia. The SOCCER database will store information on each participating COUNTRY and
PLAYER, as well as each MATCH_RESULT. It will also include information about each STADIUM, as well as which players scored goals in which matches and which
players received cards (YELLOW - warning, or RED dismissal) in which games.


3. For each COUNTRY of the 32 countries, we shall keep track of the country name (CName - unique), Continent, Confederation, Population, and another unique
attribute (surrogate key) called Cid. (The Cid will be determined by which group the country was placed in during the world cup draw - see item 7 a) below).



4. The database shall keep information about the stadiums used as venues for the matches. Each STADIUM has a stadium id (SId - a surrogate key), stadium name
(SName), City, and Capacity (max. no. of spectators).


5. For each MATCH (game) played, we will keep track of the following information: match/game id (Gld - a surrogate key), game type (GType - see description below
in item 7), date the match was played (GDate), the venue where the game was played (Stadium), first team (Team1), second team (Team2), first team score (Score1),
and second team score (Score2).


6. The game type GType indicates if it is a group game or knockout game. It can be either a group game or a knockout game as described in a) and b) below.

   
7. Initially, each of the 32 teams are assigned during the world cup draw to one of 8 groups: group A, B, C, D, E, F, G, or H. Each group has 4 teams and they play
each other round-robin manner so each group has a total of 6 games played (for example, group A has Russia, Saudi Arabia, Egypt, and Uruguay so they play the
following 6 matches: Russia v Saudi Arabia, Egypt v Uruguay, Russia v Egypt, Saudi Arabia v Uruguay, Russia v Uruguay, Saudi Arabia v Egypt). The GType for a
group game will be a single letter indicating the group name (A, B, C, D, E, F, G, or H) to which the match belonged. The team country id (surrogate key Cid - see
item 1) will be A1, A2, A3, A4, for teams in group A, and similarly for other groups.


8. The top two teams in each group will play in the knockout games - round of sixteen (X), Quarterfinals (Q), Semifinals (S), FinaL (L), Third place game (T). The letters X,
Q, S, L, T will indicate the GType for the knockout games. Although tie games (draws) are allowed for group games, they are not allowed for knockout games. So
three subtypes of knockout games are possible: regular (R - game was determined in regular time of 90 minutes); extra time (E - an extra 30 minutes were played
to determine the winner), or penalty kicks (P - if the game is tied after extra time, it is determined by alternating penalty kicks). The database will keep track of the
subtype of each knockout game.


9. The database will also keep track of which players scored goals during which matches, and which players received disciplinary cards within which matches. For each
goal scored, the player who scored it and the time of the goal is recorded in the database. See item 10 below for different types of goals.


10. There are 3 types of goals scored: regular goal (R), penalty kick goal (P), and own goal (O - an own goal is scored by a player into his own team's net by mistake).


11. The database will also keep track of which players received disciplinary cards within which matches. For each card issued by the referee, the player who received
the card and the time of the card is recorded in the database, as well as the type of the card: red card (R - results in player expulsion) or yellow card (Y - player
warning)
