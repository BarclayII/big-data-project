
import pandas as PD
import numpy as NP
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as PL
import sys

data = PD.read_csv('result2.out', header=None, sep='\t', names=['id', 'date_type', 'from_', 'to', 'crime_type', 'lon', 'lat'])

data.from_ = PD.to_datetime(data.from_, errors='coerce')
data.to = PD.to_datetime(data.to, errors='coerce')

temperature = PD.read_csv('temperature.tsv', sep='\t', header=None, names=['date', 'temperature'])

temperature.date = PD.to_datetime(temperature.date, errors='coerce')

joined = data.join(temperature.set_index('date'), on='from_', how='inner')

joined.loc[joined['crime_type'] == 'FELONY'].temperature.hist(bins=int(sys.argv[1]), histtype='step', label='FELONY')
joined.loc[joined['crime_type'] == 'MISDEMEANOR'].temperature.hist(bins=int(sys.argv[1]), histtype='step', label='MISDEMEANOR')
joined.loc[joined['crime_type'] == 'VIOLATION'].temperature.hist(bins=int(sys.argv[1]), histtype='step', label='VIOLATION')

PL.legend(loc='best')
PL.savefig('temperature.png')
PL.close()
