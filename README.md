# Chess games data analysis
Use Hadoop for analysis of Chess data on AWS

### Problem A
Return the total number of games won by White, Black, and that ended in a draw, as well as what percentage of the total games this represent. The result should look like 

Black 100 0.5

White 90 0.45

Draw 10 0.05

### Problem B
For each player, return the percentage number of games they won, lost or drew when playing White and Black. The result should look like 

Player1id White 0.5 0.4 0.1

Player1id Black 0.55 0.4 0.05

Player2id White 0.35 0.65 0

Player2id Black 0.3 0.5 0.2


### Problem C
We are trying to figure out how long (in terms of number of moves), games last. For this part, you can assume that the number of moves is given in the tag Plycount. Return the length of games sorted by frequency in the data set. The result should look like:

45 7%

32 6.9%

110 6.7%

...

Sorted by game length frequency
