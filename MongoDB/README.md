
You will need to design two document (complex object) schemas corresponding to this data:



1. The COUNTRY document will include the following data: Cname, Capital, Population, Manager (of the national score team), and a list of the players {players: Lname,
Fname, Height, DOB, is_Captain, Position, no_Yellow_cards, no_Red_cards, no_Goals, no_Assists}, plus a list of World Cup won history{Year, Host}.



2. The STADIUM document will include the following data: Stadium, city, and a list of the matches of the stadium {Match: Team1, Team2, Team1Score, Team2Score,
Date}


Your tasks for the project are as follows:


1. Write programs to extract the data needed for the two document types above (COUNTRY and STADIUM) from the flat data files, and load these documents into the
MongoDB system.


2. Write some MongoDB queries to retrieve some of the stored documents.


Queries:


1. Retrieve the list of country names that have won a world cup.


2. Retrieve the list of country names that have won a world cup and the number of world cup each has won in descending order.


3. List the Capital of the countries in increasing order of country population for countries that have population more than 100 million.


4. List the Name of the stadium which has hosted a match where the number of goals scored by a single team was greater than 4.


5. List the names of all the cities which have the name of the Stadium starting with "Estadio".


6. List all stadiums and the number of matches hosted by each stadium.


7. List the First Name, Last Name and Date of Birth of Players whose heights are greater than 198 cms.


8. List the Fname, Lname, Position and No of Goals scored by the Captain of a team who has more than 2 Yellow cards or 1 Red card.

