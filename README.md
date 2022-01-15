# Coldchain


Team CATTLEytics :: Submitted by Dr. Shari van de Pol
---

Below is CATTLEytics submission of the coldchain hackathon solution. 

To run this solution you will need python3 install

The directory structure should match the directory structure of this github project. 

```
data/Test/*.csv
data/Train/*.csv
CATTLEytics_HackAThon_Shari_van_de_Pol.py
requirements.txt
```

Prerequisites
--
To prepare your environment you must run pip install to install teh requirements (pandas)

```
pip install -r requirements.txt
```

Run
---

To run the script run the following command

```
python3 CATTLEytics_HackAThon_Shari_van_de_Pol.py
```


Output
--

  
The output of the file will be created in the same directory, predictions.txt.  As was confirmed was acceptable by Kathryn earlier, there is a line number for each line entry.  

If the arrival checks file is present and found, there will also be a  Recall, Precision, Accuracy and F1Score printed to the command line.  

Discussion
--
Initially more complex models were applied to the data in order to predict a percentage chance of having a quality issue or not having one.  Although these predictions were quite good and showed statistically significant differences based on different attributes and values, most of these had to be abandoned because of the nature of the requested output.  For example,  predicting that one group of tomatoes has a 5% chance of QC issues and another group has a 40% chance of QC issues based on an attribute, will still leave us with less than a 50% chance for either group, hence the outputs if a binary output is required would be 1's for all.  It is possible that in the future if the true goal was to predict which of each food type needs to be checked, this type of algorithm could better guide staff.  


