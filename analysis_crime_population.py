from csv import reader
import matplotlib.pyplot as plt
import numpy as np

zip_crime = "zip_vs_crime.log"
population = "2010population.csv"

crime = eval(open(zip_crime,'r').reader())
csv_reader = reader(open(population,'r'))
popu = {}
for row in csv_reader:
    popu[str(row[0])] = int(row[1])

crime_total = {}
crime_1 = {}
crime_2 = {}
crime_3 = {}

for i in crime:
    crime_1[i] = crime[i][0]
    crime_2[i] = crime[i][1]
    crime_3[i] = crime[i][2]
    crime_total[i] = crime[i][0]+crime[i][1]+crime[i][2]
    
def plot_scatter(d_crime, d_hypo):
    x=[]
    y=[]
    for i in d_crime:
        if i in d_hypo:
            x.append(d_hypo[i])
            y.append(d_crime[i])
    fit = np.polyfit(x,y,1)
    fit_fn = np.poly1d(fit) 
    plt.scatter(x,y, s=4)
    plt.plot(x,y, 'b.', x, fit_fn(x), '--k')
    covariance = np.cov(x,y,ddof=0)
    return covariance[0][1]/np.sqrt(covariance[0][0]*covariance[1][1])

plt.figure()
plot_scatter(crime_total, popu)
plt.figure()
plot_scatter(crime_1, popu)
plt.figure()
plot_scatter(crime_2, popu)
plt.figure()
plot_scatter(crime_3, popu)

plt.show()