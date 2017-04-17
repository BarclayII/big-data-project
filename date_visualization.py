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
keys = []

for row in data:
    if row[1] == 'exact' or row[1] == 'period' :
        dt.append(datetime.strptime(row[2], "%Y-%m-%d %H:%M:%S"))
        keys.append(row[0])
		
df = pd.DataFrame(np.matrix(dt).H, index=keys, columns=['Crime_Num'])

df['date'] = df[0].map(lambda x: x.strftime("%Y-%m-%d"))

rawdate = df.groupby(['date'])[0].count()
rawdate.plot()
plt.show()

df_clean = df.loc[df['year']>=2006]
df_clean['year'] = df_clean[0].map(lambda x: int(x.strftime("%Y")))
df_clean['hour'] = df_clean[0].map(lambda x: x.strftime("%H"))

cleandate = df_clean.groupby(['date'])[0].count()
cleantime = df_clean.groupby(['hour'])[0].count()

cleandate = pd.DataFrame(cleandate, index=pd.DatetimeIndex(cleandate.index), columns='Crime_Num')
pv = pd.pivot_table(cleandate, index=cleandate.index.month, columns=cleandate.index.year, values='Crime_Num', aggfunc='mean')
pv.plot()
plt.show()

cleantime.plot()
plt.show()



