###########################

#CATTLEytics Hackathon Submission
# Written by Shari van de Pol

#########
#Setup Variables


import csv
import json
import pandas as pd
import sqlite3

conn = sqlite3.connect('ColdChain')
c = conn.cursor()
print ("----------------------------------------------------------------")
print ("      Welcome to the TELUS Hackathon")
print ("")
print ("      Team CATTLEytics")
print ("      Written by Shari van de Pol")
print ("")
print ("")
print ("Starting Analysis of files.  Please ensure file names and paths are present in the python script")
print ("")
print ("")
print ("")
print ("----------------------------------------------------------------")
##############

df = pd.read_csv('data/Test/COLDCHAIN_TAGDATA.csv')
#print (df)
df.reset_index(inplace=True)
data_dict = df.to_dict('records')

df.to_sql('TagData', conn, if_exists='replace', index = False)

##############
dfTrip = pd.read_csv('data/Test/COLDCHAIN_TRIPDATA.csv')
#print (dfTrip)
dfTrip.reset_index(inplace=True)
data_dict = dfTrip.to_dict('records')

dfTrip.to_sql('TripData', conn, if_exists='replace', index = False)


##############
#  Can be commented out if you want to compare with arrival checks
try:
    df = pd.read_csv('data/Test/COLDCHAIN_ARRIVAL_CHECKS.csv')
    #print (df)
    df.reset_index(inplace=True)
    data_dict = df.to_dict('records')
    df.to_sql('ArrivalChecks', conn, if_exists='replace', index = False)
except (IOError, SyntaxError) as e:
	print("Arrival Checks file not present ")



#########
#Drop View
c.execute("drop view IF EXISTS ColdChainSummary;")

print ("Analyzing... ")
#########
#Create ColdChain View

for row in c.execute("create view ColdChainSummary as SELECT td.responseID, td.BarCode, td.SubCategory, td.Highthreshold, \
td.Lowthreshold, substr(RouteID, 0, instr( RouteID, '-')) as RouteIDFrom, strftime('%m',departure_Date) as DepartureMonth, \
substr(RouteID, ( instr( RouteID,'-') )+1, Length(RouteID)) as RouteIDTo, \
ROUND((JULIANDAY(Arrival_Date) - JULIANDAY(Departure_Date)) * 1440) AS TimeElapsed, \
sum(CASE WHEN Temperature >= (td.HighThreshold + CAST((td.HighThreshold-td.LowThreshold) AS real)/2) THEN 1 ELSE 0 END) AS aboveThreshold2, \
sum(CASE WHEN Temperature >= (td.HighThreshold + 3*CAST((td.HighThreshold-td.LowThreshold) AS real)/4) THEN 1 ELSE 0 END) AS aboveThreshold3, \
sum(case when Temperature > td.HighThreshold then 1 else 0 end) as NumAbove, \
sum(case when Temperature <= -2 then 1 else 0 end) as Freezing, \
sum(case when Temperature < td.LowThreshold then 1 else 0 end) as NumBelow, \
max(trd.Temperature) as MaxTemp FROM TagData td left join TripData \
trd on td.BarCode = trd.BarCode group by td.responseID, td.BarCode, td.SubCategory, \
td.Highthreshold, td.Lowthreshold, departure_Date;"):       
    print(row)


#########
#Send to panda dataframe with Mango work -- working!

df = pd.read_sql("select RESPONSEID, BarCode as QC_STOCKCODE, case when \
(SubCategory in ('Turkey','Ham') and NumAbove >= 800) or \
(SubCategory in ('Cheese')  and Freezing = 1) or \
(AboveThreshold2 >= 2 and SubCategory in ('Cabbage')) or  \
((AboveThreshold3 >= 30 or RouteIDTo in ('842359001206', '842359001211')) and SubCategory in ('Mushrooms')) or \
(RouteIDTo in ('842359001167') and SubCategory in ('Oranges') ) or \
(RouteIDTo in ('842359001167', '842359001210') and SubCategory in ('Corn')) or  \
(RouteIDTo in ('842359001167') and SubCategory in ('Zucchini')) or \
    (SubCategory in ('Mango') and (case when ((case  \
  when RouteIDTo = '842359001167' then  0.08 \
  when RouteIDTo = '842359001204' then -0.1 \
  when RouteIDTo = '842359001210' then  -0.13 \
  when RouteIDTo = '842359001211' then  0.50 \
  else 0 end)  + \
(case when timeelapsed <= 1000 then 0.17 \
 when timeelapsed >= 2000 and TimeElapsed < 3000 then -0.23  \
 when TimeElapsed >= 3000 then 0.03 \
 else 0 \
 end) + \
 (case when MaxTemp < 16 then -0.14  \
	when MaxTemp >= 18 and MaxTemp < 20 then 0.04  \
	when MaxTemp >= 20  then 0.1  \
	else 0 \
 end) + \
   (case when DepartureMonth in ('1','9') then -0.15 \
	when DepartureMonth in ('3', '12') then -0.03 \
	when DepartureMonth in ('7','8') then 0.07 \
	when DepartureMonth =  '10' then 0.15 \
	when DepartureMonth =  '11' then 0.04 \
	else 0 \
 end) > 0 ) then 1 else 0 end) > 0 ) \
    then 1 else 0 end as PREDICTED_ISQCISSUE \
                     from  ColdChainSummary;", con=conn)


#########
#Save to CSV

df.to_csv("Predictions.txt", sep='\t')


#########
#Delete view for Prediction Results

c.execute("drop view IF EXISTS PredictionResults;")


#########
#Create Prediction Results View to compare output of the data

for row in c.execute("Create View PredictionResults as select cc.SubCategory, cc.RESPONSEID, \
cc.BarCode as QC_STOCKCODE, ISQCISSUE, DepartureMonth, \
case when \
(cc.SubCategory in ('Turkey','Ham') and NumAbove >= 800) or \
(cc.SubCategory in ('Cheese')  and Freezing = 1) or \
(AboveThreshold2 >= 2 and cc.SubCategory in ('Cabbage')) or  \
((AboveThreshold3 >= 30 or RouteIDTo in ('842359001206', '842359001211')) and cc.SubCategory in ('Mushrooms')) or \
(RouteIDTo in ('842359001167') and cc.SubCategory in ('Oranges') ) or \
(RouteIDTo in ('842359001167', '842359001210') and cc.SubCategory in ('Corn')) or  \
(RouteIDTo in ('842359001167') and cc.SubCategory in ('Zucchini')) or \
    (cc.SubCategory in ('Mango') and (case when ((case  \
  when RouteIDTo = '842359001167' then  0.08 \
  when RouteIDTo = '842359001204' then -0.1 \
  when RouteIDTo = '842359001210' then  -0.13 \
  when RouteIDTo = '842359001211' then  0.50 \
  else 0 end)  + \
(case when timeelapsed <= 1000 then 0.17 \
 when timeelapsed >= 2000 and TimeElapsed < 3000 then -0.23  \
 when TimeElapsed >= 3000 then 0.03 \
 else 0 \
 end) + \
 (case when MaxTemp < 16 then -0.14  \
	when MaxTemp >= 18 and MaxTemp < 20 then 0.04  \
	when MaxTemp >= 20  then 0.1  \
	else 0 \
 end) + \
   (case when DepartureMonth in ('1','9') then -0.15 \
	when DepartureMonth in ('3', '12') then -0.03 \
	when DepartureMonth in ('7','8') then 0.07 \
	when DepartureMonth =  '10' then 0.15 \
	when DepartureMonth =  '11' then 0.04 \
	else 0 \
 end) > 0 ) then 1 else 0 end) > 0 ) \
    then 1 else 0 end as PREDICTED_ISQCISSUE from  ColdChainSummary cc left join ArrivalChecks ac \
    on cc.ResponseID = ac.ResponseID ;"):  
    print(row)

listOfTables = c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ArrivalChecks'").fetchall()

if listOfTables == []:
    print('Arrival Checks Data is not available for comparison')
else:
  	  print ("----------------------------------------------------------------")
  	  print ("Summary of Results ")
  	  print ("Accuracy, Precision, Recall and F1Score are:  ")
  	  #########
  	  #Find Recall, Precision, Accuracy and F1Score
  	  
  	  for row in c.execute("select *, 2*(Recall * Precision) / (Recall + Precision) as F1Score from ( select \
  	  (cast(RealB as real) + RealG)/(RealB + RealG + FalseNeg + FalsePos) * 100  as Accuracy, \
  	   cast(RealB as real)/(RealB + FalsePos)* 100 as Precision, \
  	   cast(RealB as real)/(FalseNeg + RealB)* 100 as Recall \
  	  from (  \
  	  select   \
  	  sum(case when ActualB = 1 and PredB = 1 then 1 end) as RealB,   \
  	  sum(case when ActualB = 0 and PredB = 0 then 1 end) as RealG,   \
  	  sum(case when ActualB = 1 and PredB = 0 then 1 end) as FalseNeg,  \
  	  sum(case when ActualB = 0 and PredB = 1 then 1 end) as FalsePos  \
  	   from (  \
  	     select ResponseID, QC_StockCode, \
  	  PREDICTED_ISQCISSUE as PredB, ISQCISSUE as ActualB from  PredictionResults \
  	         ) \
  	   as q1  \
  	   ) as q2 \
  	   ) as q3;"):  
  	   print(row)  
print ("----------------------------------------------------------------")

#########
#Clean up Views
c.execute("drop table IF EXISTS ArrivalChecks;")
c.execute("drop view IF EXISTS PredictionResults;")
c.execute("drop view IF EXISTS ColdChainSummary;")   

print("Analysis Complete.  See the Prediction.txt for details.")


