from bs4 import BeautifulSoup
from urllib.request import urlopen
from csv import reader
import sys
from warnings import catch_warnings

if len(sys.argv) <= 1 :
    inputfile = 'zipcodes.csv'
else:
    inputfile = sys.argv[1]    
data = reader(open(inputfile, 'r'))
zips = set()
for row in data:
    zips.add(row[0])

if len(sys.argv) <= 2 :
    outputfile = 'result.csv'
else:
    outputfile = sys.argv[2]    
f = open(outputfile, 'w')

count = 0
fail = 0
url = "https://www.incomebyzipcode.com/newyork/"
for zipcode in zips:
    try:
        soup = BeautifulSoup(urlopen(url+str(zipcode)))
    except:
        fail += 1
        continue
    result = soup.find_all('div','data')
    if len(result) != 5:
        fail += 1
        continue
    row = zipcode
    for gomi in result[0:4]:
        row += ',' + gomi.td.string.replace('$','').replace(',','').replace('%','')
    f.write(row)
    print(row)
    count += 1

f.close()
print("Count: %d\tFails: %d", count, fail)