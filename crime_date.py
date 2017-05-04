import sys

from csv import reader
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

if len(sys.argv) <= 1 :
    inputfile = 'C:/Users/Neutrino/Google Drive/BigData/result2.out'
else:
    inputfile = sys.argv[1]
    
infile = open(inputfile, 'r')
data = reader(infile, delimiter='\t')

dt = []
tp = []
keys = []

for row in data:
    if row[1] == 'exact' or row[1] == 'period' :
        dt.append(datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S"))
        tp.append(row[4])
        keys.append(row[0])
		
df = pd.DataFrame(np.matrix([dt,tp]).H, index=keys, columns=['date','type'])

df['year'] = df['date'].map(lambda x: int(x.strftime("%Y")))
df['day'] = df['date'].map(lambda x: x.strftime("%Y-%m-%d"))
df = df.loc[df['year']>=2006]

countResult = df_clean.groupby(['day'])['type'].value_counts().to_frame().unstack(level=-1)
countResult['total'] = df_clean.groupby(['day'])['type'].count()

countResult.to_csv('count.csv')


