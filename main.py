# -*- coding: utf-8 -*-
import requests
import sqlite3
from lxml import html
import codecs
import sys
import json
import threading
from datetime import datetime
import os
import os.path
import time
import pyodbc
import importlib
import codecs
importlib.reload(sys)
start_time=datetime.now()
db_file = os.getcwd()+'/sfc-database.accdb'
user = ''
password = ''
odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;UID=%s;PWD=%s' % (db_file, user, password)

db = pyodbc.connect(odbc_conn_str)
cnn=db.cursor()
cnn.execute('delete * from [Licence Details  (C)]')
cnn.execute("ALTER TABLE [Licence Details  (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete * from [Business addresses_Addresses  (C)]')
cnn.execute("ALTER TABLE [Business addresses_Addresses  (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete * from [Email address_Addresses  (C)]')
cnn.execute("ALTER TABLE [Email address_Addresses  (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete * from [Website_Addresses (C)]')
cnn.execute("ALTER TABLE [Website_Addresses (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete * from [Complaints Officers  (C)]')
cnn.execute("ALTER TABLE [Complaints Officers  (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Conditions (C)]')
cnn.execute("ALTER TABLE [Conditions (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Public disciplinary actions (C)]')
cnn.execute("ALTER TABLE [Public disciplinary actions (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Previous name (C)]')
cnn.execute("ALTER TABLE [Previous name (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Licence record (C)]')
cnn.execute("ALTER TABLE [Licence record (C)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Licence Details (ID)]')
cnn.execute("ALTER TABLE [Licence Details (ID)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Licence record (ID)]')
cnn.execute("ALTER TABLE [Licence record (ID)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Public disciplinary actions (ID)]')
cnn.execute("ALTER TABLE [Public disciplinary actions (ID)] ALTER COLUMN [id] COUNTER(1, 1)")
cnn.execute('delete from [Conditions (ID)]')
cnn.execute("ALTER TABLE [Conditions (ID)] ALTER COLUMN [id] COUNTER(1, 1)")
db.commit()
file=os.getcwd()+'/HKSFC.db'
try: 
	if os.path.isfile(file): os.remove(file)
except: pass
db=sqlite3.connect('HKSFC.db')
db.text_factory = lambda x: str(x, "utf-8", "ignore")
cnn = db.cursor()
sql='''create table if not exists persons (
id integer primary key autoincrement,
ceref text default null,
name text default null,
nameChi text default null,
category text default null,
execute integer default null
)
'''
cnn.execute(sql)
db.commit()
sql='create index if not exists id_person on persons (ceref)'
cnn.execute(sql)
db.commit()
db.close()
url='https://www.sfc.hk/publicregWeb/searchByRa?locale=en'
nameStartLetter='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
roleType=['individual','corporation']

#%%
def request_url(url):
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36'}
    t = 1
    while True:
        try:
            page = requests.get(url, timeout=(120, 120), headers=header)
            if page.status_code == 200:
                t = 0
                break
            else:
                t += 1
                if t ==5:
                    page = None
                    break
                print('May be IP blocked. Waiting 20 min ...')
                time.sleep(20 * 60)
        except:
            t += 1
            if t == 5:
                page=None
                break
            time.sleep(3)
    return page

#%%
def persons(data):
    global lock, total
    print(threading.current_thread().getName() + ' - ' + str(total) + '      ' + chr(13))
    db = sqlite3.connect('HKSFC.db')
    db.text_factory = lambda x: str(x, "utf-8", "ignore")
    cnn = db.cursor()
    url = 'https://www.sfc.hk/publicregWeb/searchByRaJson'
    t=1
    header={
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Host': 'www.sfc.hk',
    'Origin': 'https://www.sfc.hk',
    'Referer': 'https://www.sfc.hk/publicregWeb/searchByRa'
    }
    while True:
        try:
            page = requests.post(url, data=data, timeout=(120,120), headers=header)

            #f=codecs.open('x.txt','w', 'utf8')
            #f.write(page.text)
            #f.close()

            if page.status_code == 200: 
            	t=0
            	break
        except requests.exceptions.RequestException as e:
            t+=1
            if t==5: break
            time.sleep(3)
    if t==0:
        d = json.loads(page.text)
        items = d['items']
        lock.acquire()
        for h in items:
            if (h['isIndi']==True and data['roleType']=='individual') or (h['isCorp']==True and data['roleType']=='corporation'):
                cnn.execute("select id from persons where ceref='"+h['ceref']+"'")
                rec=cnn.fetchone()
                if rec is None:
                    sql = "insert into persons (ceref, name, nameChi, category) values (?,?,?,?)"
                    cnn.execute(sql, (h['ceref'], h['name'], h['nameChi'], r))
        db.commit()
        lock.release()
    db.close()

#%%
def scrape(data):
    global lock, start_time
    print(threading.current_thread().getName() + ' - ' + str(total) + '      ' + chr(13))
    db_file = os.getcwd()+'\\sfc-database.accdb'
    user = ''
    password = ''
    odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s' % (db_file,)
    db = pyodbc.connect(odbc_conn_str)
    cnn=db.cursor()
    if data[4]=='individual':
        rec=cnn.execute("select [CE_Number(ID)] from [Licence Details (ID)] where [CE_Number(ID)]=?",(data[1],)).fetchone()
        if rec is not None: return
        url='https://www.sfc.hk/publicregWeb/indi/'+data[1]+'/details'
        page=request_url(url)
        if page is not None:
            z=page.text.find('Date Of licence')
            if z>0:
                z1 = page.text.find('(',z)
                z2 = page.text.find(')',z1)
                ss=page.text[z1:z2].split(',')[1].replace("'",'').strip()
                date_licence = None
                if len(ss.split('/'))==3:
                    try:
                        date_licence=datetime.strptime(ss,'%d/%m/%Y').date()
                    except:
                        print ('#######',ss)
                remarks=None
                name = data[2].split(' ')
                name2 = name[0]
                name.pop(0)
                name1 = ' '.join(name)
                lock.acquire()
                sql="insert into [Licence Details (ID)] ([English Last Name], [English First Name], [Chinese Name], [Date of licence], [Remarks], [CE_Number(ID)]) values (?,?,?,?,?,?)"
                values=(name1, name2, data[3], date_licence, remarks, data[1])
                cnn.execute(sql, values)
                db.commit()
                lock.release()
        url='https://www.sfc.hk/publicregWeb/indi/'+data[1]+'/licenceRecord'
        page=request_url(url)
        if page is not None:
            z=page.text.find('var licRecordData = ')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    if i['lcRole']=='RE': lcRole='Representative'
                    else: lcRole='Responsible Officers'
                    actDesc=i['regulatedActivity']['actDesc']
                    effectiveDate=i['effectivePeriodList'][-1]['effectiveDate']
                    endDate=i['effectivePeriodList'][-1]['endDate']
                    if effectiveDate is not None: effectiveDate = datetime.strptime(effectiveDate, '%b %d, %Y %I:%M:%S %p')
                    if endDate is not None: endDate = datetime.strptime(endDate, '%b %d, %Y %I:%M:%S %p')
                    prinCeRef=i['prinCeRef']
                    sql="insert into [Licence record (ID)] ([Licence role], [Regulated Activity], [Effective period start date], [Effective period end date], [CE_Number], [CE_Number(ID)]) values (?,?,?,?,?,?)"
                    values=(lcRole, actDesc, effectiveDate, endDate, prinCeRef, data[1])
                    cnn.execute(sql, values)
                db.commit()
                lock.release()

        url='https://www.sfc.hk/publicregWeb/indi/'+data[1]+'/disciplinaryAction'
        page = request_url(url)
        if page is not None:
            z=page.text.find('var disRemarkData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    d=i['actnDate'].split(' ')
                    if d[0]=='Jan': mm=1
                    if d[0]=='Feb': mm=2
                    if d[0]=='Mar': mm=3
                    if d[0]=='Apr': mm=4
                    if d[0]=='May': mm=5
                    if d[0]=='Jun': mm=6
                    if d[0]=='Jul': mm=7
                    if d[0]=='Aug': mm=8
                    if d[0]=='Sep': mm=9
                    if d[0]=='Oct': mm=10
                    if d[0]=='Nov': mm=11
                    if d[0]=='Dec': mm=12
                    dd=int(d[1].replace(',',''))
                    yy=int(d[2])
                    actnDate=datetime(yy,mm,dd)
                    codeDesc=i['codeDesc']
                    engDocSeq='https://www.sfc.hk/publicregWeb/displayFile?docno='+ss[0]['engDocSeq']
                    sql="insert into [Public disciplinary actions (C)] ([CE_Number], [Date of action], [Action taken], [Press Releases]) values (?,?,?,?)"
                    values=(data[1], actnDate, codeDesc, engDocSeq)
                    cnn.execute(sql, values)
                db.commit()
                lock.release()
        url='https://www.sfc.hk/publicregWeb/indi/'+data[1]+'/conditions'
        page = request_url(url)
        if page is not None:
            z=page.text.find('var indData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                text=page.text[z1:z2+1]
                ss=json.loads(text)
                if len(ss)>0:
                    lock.acquire()
                    for i in ss:
                        d=i['effDate'].split(' ')
                        if d[0]=='Jan': mm=1
                        if d[0]=='Feb': mm=2
                        if d[0]=='Mar': mm=3
                        if d[0]=='Apr': mm=4
                        if d[0]=='May': mm=5
                        if d[0]=='Jun': mm=6
                        if d[0]=='Jul': mm=7
                        if d[0]=='Aug': mm=8
                        if d[0]=='Sep': mm=9
                        if d[0]=='Oct': mm=10
                        if d[0]=='Nov': mm=11
                        if d[0]=='Dec': mm=12
                        dd=int(d[1].replace(',',''))
                        yy=int(d[2])
                        effDate=None
                        try:
                            effDate=datetime(yy,mm,dd)
                        except:
                            print ('>>>>>>>>>>',url)
                            print ('data=',d,text)

                        conditionDtl=i['conditionDtl']
                        sql="insert into [Conditions (ID)] ([Effective date], [Licensing conditions], [CE_Number(ID)]) values (?,?,?)"
                        values=(effDate, conditionDtl, data[1])
                        cnn.execute(sql, values)
                    db.commit()
                    lock.release()

    if data[4]=='corporation':
        rec=cnn.execute("select [CE_Number] from [Licence Details  (C)] where [CE_Number]=?",(data[1],)).fetchone()
        if rec is not None: return
        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/details'
        page=request_url(url)
        if page is not None:
            #f=codecs.open('corporation_licence'+data[1]+'.html','w','utf-8')
            #f.write(page.text)
            #f.close()
            z=page.text.find('Date of licence')
            if z>0:
                z1 = page.text.find('(',z)
                z2 = page.text.find(')',z1)
                ss=page.text[z1:z2].split(',')[1].replace("'",'').strip()
                date_licence = None
                sss=ss.split('/')
                if len(sss)==3:    date_licence=datetime(int(sss[2]),int(sss[1]),int(sss[0]))
                sql="insert into [Licence Details  (C)] ([English Company Name], [Chinese Company Name], [Date of License], [Type], [CE_Number]) values (?,?,?,?,?)"
                values=(data[2], data[3], date_licence, 'corporation', data[1])
                lock.acquire()
                cnn.execute(sql, values)
                db.commit()
                lock.release()
        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/addresses'
        page = request_url(url)
        if page is not None:
            fullAddress=None
            email=None
            website=None
            z=page.text.find('var addressData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    fullAddress=i['fullAddress']
                    sql="insert into [Business addresses_Addresses  (C)] ([Business addresses], [CE_Number]) values (?,?)"
                    values=(fullAddress, data[1])
                    cnn.execute(sql, values)
                db.commit()
                lock.release()
            z=page.text.find('var emailData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    try: email=i['email']
                    except: email=None
                    sql="insert into [Email address_Addresses  (C)] ([Email address], [CE_Number]) values (?,?)"
                    values=(email, data[1])
                    cnn.execute(sql, values)
                db.commit()
                lock.release()
            z=page.text.find('var websiteData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    try: website=i['website']
                    except: website=None
                    sql="insert into [Website_Addresses (C)] ([Website address], [CE_Number]) values (?,?)"
                    values=(website, data[1])
                    cnn.execute(sql, values)
                db.commit()
                lock.release()
        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/co'
        page = request_url(url)
        if page is not None:
            z=page.text.find('var cofficerData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    tel=i['tel']
                    fax=i['fax']
                    email=i['email']
                    address=i['address']['fullAddress']
                    sql="insert into [Complaints Officers  (C)] ([CE_Number], [Telephone], [Fax], [Email address],[Correspondence address]) values (?,?,?,?,?)"
                    values=(data[1], tel, fax, email, address)
                    cnn.execute(sql, values)
                db.commit()
                lock.release()
        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/conditions'
        page = request_url(url)
        if page is not None:
            z=page.text.find('var condData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                text=page.text[z1:z2+1]
                ss=json.loads(text)
                lock.acquire()
                for i in ss:
                    d=i['effDate'].split(' ')
                    if d[0]=='Jan': mm=1
                    if d[0]=='Feb': mm=2
                    if d[0]=='Mar': mm=3
                    if d[0]=='Apr': mm=4
                    if d[0]=='May': mm=5
                    if d[0]=='Jun': mm=6
                    if d[0]=='Jul': mm=7
                    if d[0]=='Aug': mm=8
                    if d[0]=='Sep': mm=9
                    if d[0]=='Oct': mm=10
                    if d[0]=='Nov': mm=11
                    if d[0]=='Dec': mm=12
                    dd=int(d[1].replace(',',''))
                    yy=int(d[2])
                    effDate=datetime(yy,mm,dd)
                    conditionDtl=i['conditionDtl']
                    sql="insert into [Conditions (C)] ([Effective date], [Licensing conditions], [CE_Number]) values (?,?,?)"
                    values=(effDate, conditionDtl, data[1])
                    cnn.execute(sql, values)
                db.commit()
                lock.release()

        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/da'
        page = request_url(url)
        if page is not None:
            z=page.text.find('var disRemarkData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    d=i['actnDate'].split(' ')
                    if d[0]=='Jan': mm=1
                    if d[0]=='Feb': mm=2
                    if d[0]=='Mar': mm=3
                    if d[0]=='Apr': mm=4
                    if d[0]=='May': mm=5
                    if d[0]=='Jun': mm=6
                    if d[0]=='Jul': mm=7
                    if d[0]=='Aug': mm=8
                    if d[0]=='Sep': mm=9
                    if d[0]=='Oct': mm=10
                    if d[0]=='Nov': mm=11
                    if d[0]=='Dec': mm=12
                    dd=int(d[1].replace(',',''))
                    yy=int(d[2])
                    actnDate=datetime(yy,mm,dd)
                    codeDesc=i['codeDesc']
                    engDocSeq='https://www.sfc.hk/publicregWeb/displayFile?docno='+ss[0]['engDocSeq']
                    sql="insert into [Public disciplinary actions (C)] ([CE_Number], [Date of action], [Action taken], [Press Releases]) values (?,?,?,?)"
                    values=(data[1], actnDate, codeDesc, engDocSeq)
                    cnn.execute(sql, values)
                db.commit()
                lock.release()

        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/prev_name'
        page = request_url(url)
        if page is not None:
            z=page.text.find('var prevNameData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    d=i['changeDate'].split(' ')
                    if d[0]=='Jan': mm=1
                    if d[0]=='Feb': mm=2
                    if d[0]=='Mar': mm=3
                    if d[0]=='Apr': mm=4
                    if d[0]=='May': mm=5
                    if d[0]=='Jun': mm=6
                    if d[0]=='Jul': mm=7
                    if d[0]=='Aug': mm=8
                    if d[0]=='Sep': mm=9
                    if d[0]=='Oct': mm=10
                    if d[0]=='Nov': mm=11
                    if d[0]=='Dec': mm=12
                    dd=int(d[1].replace(',',''))
                    yy=int(d[2])
                    changeDate=datetime(yy,mm,dd)
                    englishName=i['englishName']
                    chineseName=i['chineseName']
                    sql="insert into [Previous name (C)] ([CE_Number], [Valid until], [English name], [Chinese name]) values (?,?,?,?)"
                    values=(data[1], changeDate, englishName, chineseName)
                    cnn.execute(sql, values)
                db.commit()
                lock.release()

        url='https://www.sfc.hk/publicregWeb/corp/'+data[1]+'/licences'
        page=request_url(url)
        if page is not None:
            z=page.text.find('var licRecordData =')
            if z>0:
                z1=page.text.find('[',z)
                z2 = page.text.find('];', z1)
                ss=json.loads(page.text[z1:z2+1])
                lock.acquire()
                for i in ss:
                    if i['lcType']=='E': lcType='Registered Institution'
                    else: lcType='Licensed Corporation'
                    actDesc=i['regulatedActivity']['actDesc']
                    effectiveDate=i['effectivePeriodList'][-1]['effectiveDate']
                    endDate=i['effectivePeriodList'][-1]['endDate']
                    if effectiveDate is not None:
                        d=effectiveDate.split(' ')
                        if d[0]=='Jan': mm=1
                        if d[0]=='Feb': mm=2
                        if d[0]=='Mar': mm=3
                        if d[0]=='Apr': mm=4
                        if d[0]=='May': mm=5
                        if d[0]=='Jun': mm=6
                        if d[0]=='Jul': mm=7
                        if d[0]=='Aug': mm=8
                        if d[0]=='Sep': mm=9
                        if d[0]=='Oct': mm=10
                        if d[0]=='Nov': mm=11
                        if d[0]=='Dec': mm=12
                        dd=int(d[1].replace(',',''))
                        yy=int(d[2])
                        effectiveDate=datetime(yy,mm,dd)
                    if endDate is not None:
                        d=endDate.split(' ')
                        if d[0]=='Jan': mm=1
                        if d[0]=='Feb': mm=2
                        if d[0]=='Mar': mm=3
                        if d[0]=='Apr': mm=4
                        if d[0]=='May': mm=5
                        if d[0]=='Jun': mm=6
                        if d[0]=='Jul': mm=7
                        if d[0]=='Aug': mm=8
                        if d[0]=='Sep': mm=9
                        if d[0]=='Oct': mm=10
                        if d[0]=='Nov': mm=11
                        if d[0]=='Dec': mm=12
                        dd=int(d[1].replace(',',''))
                        yy=int(d[2])
                        endDate=datetime(yy,mm,dd)
                    sql="insert into [Licence record (C)] ([Licence type], [Regulated Activity], [Effective period start date], [Effective period end date], [CE_Number]) values (?,?,?,?,?)"
                    values=(lcType, actDesc, effectiveDate, endDate, data[1])
                    cnn.execute(sql, values)
                db.commit()
                lock.release()
    lock.acquire()
    db=sqlite3.connect('HKSFC.db')
    db.text_factory = lambda x: str(x, "utf-8", "ignore")
    cnn = db.cursor()
    cnn.execute("update persons set execute=1 where ceref=?",(data[1],))
    db.commit()
    db.close()
    lock.release()
#%%    
max_thread=30
lock=threading.Lock()
index=0
total=20*len(nameStartLetter)
for r in roleType:
    for i in range(1,11):
        for j in nameStartLetter:
            data={
                'licstatus':'active',
                'ratype': i,
                'roleType': r,
                'nameStartLetter': j,
                'page':'1',
                'start':'0',
                'limit': '10000'
            }
            index+=1
            t = threading.Thread(target=persons, name='PERSON_' + str(index), args=(data,))
            t.start()   
            time.sleep(0.2) 
            while threading.activeCount() > max_thread: time.sleep(0.5)

while threading.activeCount() > 1:
    print(datetime.now().strftime('%H:%M:%S')+" person thread waiting: remaining "+str(threading.activeCount()))
    time.sleep(5)

db=sqlite3.connect('HKSFC.db')
#db.text_factory = lambda xx: str(xx, "utf-8", "ignore")
cnn = db.cursor()
cnn.execute("select * from persons")
rec=cnn.fetchall()
db.close()
total=len(rec)
index=0
for i in rec:
    index += 1
    t = threading.Thread(target=scrape, name='DATA_' + str(index), args=(i,))
    t.start()
    time.sleep(0.2)
    while threading.activeCount() > max_thread: time.sleep(0.5)
while threading.activeCount() > 1:
    print(datetime.now().strftime('%H:%M:%S')+" data thread waiting: remaining "+str(threading.activeCount()))
    time.sleep(5)
