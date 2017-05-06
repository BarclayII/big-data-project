
import pandas as PD
import numpy as NP
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as PL

with open('zip_vs_crime.log') as f:
    zip_crime = f.readline().strip()

zip_crime = eval(zip_crime)

income_index = ['median', 'average', 'percapita', 'highincome']

income = PD.read_csv('result.csv')
zip_income = {
        int(income.loc[i].zipcode): [
            income.loc[i, idx] for idx in income_index
            ]
        for i in range(income.shape[0])
        }

zip_crime_avail = {int(k): zip_crime[k] for k in zip_crime if sum(zip_crime[k]) != 0}
zip_income_avail = {int(k): zip_income[k] for k in zip_income if sum(zip_income[k]) != 0}

zip_avail = set(zip_crime_avail) & set(zip_income_avail)
zip_crime_avail = {k: zip_crime_avail[k] for k in zip_avail}
zip_income_avail = {k: zip_income_avail[k] for k in zip_avail}

fig, ax = PL.subplots(2, 2)
fig.set_size_inches(15, 10)

for i, idx in enumerate(income_index):
    viol, mis, fel = [
            [(zip_income_avail[k][i], zip_crime_avail[k][j]) for k in zip_avail]
            for j in range(3)
            ]
    inc = zip(*viol)[0]
    viol, mis, fel = (NP.array(zip(*k)[1]) for k in [viol, mis, fel])
    axx = i / 2
    axy = i % 2
    ax[axx, axy].set_xlabel('%s income' % idx)
    ax[axx, axy].set_ylabel('# of crimes')
    ax[axx, axy].scatter(inc, viol, color='r', marker='.', label='Violation')
    ax[axx, axy].scatter(inc, mis, color='g', marker='.', label='Misdemeanor')
    ax[axx, axy].scatter(inc, fel, color='b', marker='.', label='Felony')
    ax[axx, axy].scatter(inc, viol + mis + fel, color='k', marker='.', label='Total')
    ax[axx, axy].legend(loc='best')
    ax[axx, axy].set_xlim(left=0)
    ax[axx, axy].set_ylim(bottom=0)

fig.savefig('crime_vs_income.png')
PL.close(fig)
