#!/usr/bin/python
import mysql.connector
import datetime
import gzip
import shutil
import collections
from datetime import timedelta, date,datetime
import os,sys
from subprocess import Popen, PIPE



Const=collections.namedtuple('Const','Max_days Min_days')
Const=Const(109,0);

def TableDumpDayWise( tableName,startDay_date,endDay_date,dumpFile,dumpLocationWithFileName):

    db = mysql.connector.connect(host='localhost', database='purple',user='root', password='Takay1#$ane', port=3306)
    con = db.cursor()
    query="select * into outfile %s fields terminated by ',' enclosed by '`' from "+tableName+" where starttime >= %s and starttime < %s limit 0,10;"
    con.execute (query,(dumpLocationWithFileName,startDay_date,endDay_date))
    print(dumpFile+" Created.")
    out_file=dumpLocationWithFileName+'.gzip'
    File_zip(dumpLocationWithFileName,out_file)


def File_zip(source_file,out_file):
	f_in=open(source_file)
	f_out=gzip.open(out_file, 'wb')
        f_out.writelines(f_in)
	f_out.close()
	f_in.close()
	print(out_file+" Zipped.")
	File_delete(source_file)



def File_delete(source_file):
    os.remove(source_file)     


def DateWiseDump(startDate,endDate,tableName,location_entry):
    noOfDays=(endDate-startDate).days+1
    for n in range(0,noOfDays):
        startDay_date=startDate+timedelta(n)
        endDay_date=startDay_date+timedelta(1)
        dumpFile="backup"+startDay_date.strftime("%Y-%m-%d")+".log"
        dumpLocationWithFileName=os.path.join(location_entry,dumpFile)
        if(os.path.isfile(dumpLocationWithFileName)):
            print(dumpLocationWithFileName+" DumpFile Already Exists") 
            os.remove(dumpLocationWithFileName);
        if(os.path.isfile(dumpLocationWithFileName+'.gzip')):
            os.remove(dumpLocationWithFileName+'.gzip');
        TableDumpDayWise(tableName,startDay_date.strftime("%Y-%m-%d"),endDay_date.strftime("%Y-%m-%d"),dumpFile,dumpLocationWithFileName)
                


def DataBaseDump():
    username='root'
    password='Takay1#$ane'
    hostname='localhost'
    database='purple'
    i = datetime.now()
    filename="D:/TestDume"+i.strftime("%Y-%m-%d")+".log"
    mysqldump_cmd = "mysqldump -u"+username+" -p"+password+" "+database+" --ignore-table "+tableName+".table> "+filename
    os.system(mysqldump_cmd)
    print("Database Dump Complete.")
   
tableName="cdrloaded"

def main():
    option =raw_input("For Day Wise Dump Press 1:\n For Whole Database Dump Withou cdrloaded Table Press 2:\n")
    if int (option)== 1:
        sdate_entry = str(raw_input('Enter Start date in YYYY-MM-DD format:\n'))
        syear, smonth, sday = map(int, sdate_entry.split('-'))
        startDate=date(syear,smonth,sday);

        edate_entry = str(raw_input('Enter End date in YYYY-MM-DD format:\n'))
        eyear, emonth, eday = map(int, edate_entry.split('-'))
        endDate = date(eyear, emonth, eday)
        location_entry =raw_input('Enter Location:\n')
        DateWiseDump(startDate,endDate,tableName,location_entry);
    if int (option)==2:
        DataBaseDump();
  
if __name__== "__main__":
  main()


 







