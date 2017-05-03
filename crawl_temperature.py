import lxml.etree as ETREE
import urllib2
import datetime
import time

def find_temperature(string):
    tree = ETREE.fromstring(string, parser=ETREE.HTMLParser())

    body = tree.find('body')
    div = [d for d in body.findall('div') if d.attrib.get('id', None) == 'content-wrap'][0]
    div = [d for d in div.findall('div') if d.attrib.get('id', None) == 'inner-wrap'][0]
    section = [s for s in div.findall('section')
               if s.attrib.get('id', None) == 'inner-content' and s.attrib['role'] == 'main'][0]
    div = [d for d in section.findall('div') if d.attrib.get('class', None) == 'mainWrapper'][0]
    div = [d for d in div.findall('div') if d.attrib.get('class', None) == 'row collapse'][1]
    div = [d for d in div.findall('div') if d.attrib.get('class', None) == 'column large-8 right-spacing'][0]
    table = [t for t in div.findall('table') if t.attrib.get('id', None) == 'historyTable'][0]
    tbody = table.find('tbody')
    tr = [r for r in tbody.findall('tr')][1]
    td = [d for d in tr.findall('td')][1]
    span = [s for s in td.findall('span') if s.attrib.get('class', None) == 'wx-data'][0]
    span = [s for s in span.findall('span') if s.attrib.get('class', None) == 'wx-value'][0]
    return span.text.strip()

template = 'https://www.wunderground.com/history/airport/KNYC/%y/%m/%d/DailyHistory.html?req_city=New+York&req_state=NY&req_statename=New+York&reqdb.zip=10001&reqdb.magic=8&reqdb.wmo=99999&MR=1'

current_date = datetime.datetime(2011, 1, 1)
end_date = datetime.datetime(2015, 12, 31)

while current_date <= end_date:
    url = (
            template
            .replace('%y', str(current_date.year))
            .replace('%m', str(current_date.month))
            .replace('%d', str(current_date.day))
            )
    print '%s\t%s' % (str(current_date), find_temperature(urllib2.urlopen(url).read()))
    current_date += datetime.timedelta(1)
    time.sleep(1)
