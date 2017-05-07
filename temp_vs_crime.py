
import pandas as PD
import numpy as NP
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as PL

data = PD.read_csv('result2.out', header=None, sep='\t', names=['id', 'date_type', 'from_', 'to', 'crime_type', 'lon', 'lat'])

data.from_ = PD.to_datetime(data.from_, errors='coerce')
data.to = PD.to_datetime(data.to, errors='coerce')

temperature = PD.read_csv('temperature.tsv', sep='\t', header=None, names=['date', 'temperature'])

temperature.date = PD.to_datetime(temperature.date, errors='coerce')

counts = (
        data
        .join(temperature.set_index('date'), on='from_', how='inner')
        .groupby(['from_', 'crime_type'])
        .count()['id']
        )

PL.xlabel('temperature (F)')
PL.ylabel('# of crimes')
d = PD.DataFrame(counts[:, 'FELONY']).join(temperature.set_index('date'), how='inner')
PL.scatter(d['temperature'], d['id'], color='r', marker=',', label='Felony')
d = PD.DataFrame(counts[:, 'MISDEMEANOR']).join(temperature.set_index('date'), how='inner')
PL.scatter(d['temperature'] + 0.1, d['id'] + 0.1, color='g', marker='+', label='Misdemeanor')
d = PD.DataFrame(counts[:, 'VIOLATION']).join(temperature.set_index('date'), how='inner')
PL.scatter(d['temperature'] + 0.2, d['id'] + 0.2, color='b', marker='x', label='Violation')
PL.legend(loc='best')
PL.savefig('temperature.png')
PL.close()
