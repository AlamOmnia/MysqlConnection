import mysql.connector
import datetime
import gzip
import shutil
import collections
from datetime import timedelta, date,datetime
import os,sys,subprocess
from time import sleep


Const=collections.namedtuple('Const','Host User Password Port')
Const=Const('localhost','root','****',3306);
ignoreTableList=['cdrloaded','cdrerror']
specialChars=['$'];

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '='):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(length * iteration / float(total)))
    bar = '=' * filled_length + '-' * (length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar,percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()




def getPasword(mode):
    if mode=="mysql":
        str=Const.Password
    if mode=="shell":
        for c in specialChars:
            if c in Const.Password:
                str=Const.Password
                strList=list(str)
                strList.insert(7,"\\")
                str="".join(strList)
                
    return str
  
 

def TableDumpDayWise( tableName,startDay_date,endDay_date,dumpFile,dumpLocationWithFileName,database):

    db = mysql.connector.connect(host=Const.Host, user=Const.User,password=getPasword("mysql"),port=Const.Port)
    con = db.cursor()
    query="select * into outfile %s fields terminated by ',' enclosed by '`' from "+database+"."+tableName+" where starttime >= %s and starttime < %s limit 0,10000;"
   # print(query)
    con.execute (query,(dumpLocationWithFileName,startDay_date,endDay_date))
    #print(dumpFile+" Created.")
    out_file=dumpLocationWithFileName+'.gzip'
    File_zip(dumpLocationWithFileName,out_file)


def DateWiseDump(startDate,endDate,tableName,location_entry,database):
    noOfDays=(endDate-startDate).days+1
    printProgressBar(0, noOfDays, prefix = 'Progress:', suffix = 'Complete', length = 50)
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
        TableDumpDayWise(tableName,startDay_date.strftime("%Y-%m-%d"),endDay_date.strftime("%Y-%m-%d"),dumpFile,dumpLocationWithFileName,database)
        sleep(0.1)
        printProgressBar(n + 1, noOfDays, prefix = 'Progress:', suffix = 'Complete', length = 50)         


def DataBaseDump(file_Name,ignoreTableList,database):
    Temp= " --ignore-table "
    Tables=[Temp+s+".table" for s in ignoreTableList]
    filename="/tmp/"+file_Name+".log.gz"
    mysqldump_cmd = "mysqldump -u"+Const.User+" -p"+getPasword("shell")+" "+database+" ".join(Tables)+"|pv | gzip -9 > "+filename
    subprocess.Popen(mysqldump_cmd,stdout=subprocess.PIPE, shell=True) 
    print("Dumpdone!")   
    
   
def File_zip(source_file,out_file):
	f_in=open(source_file)
	f_out=gzip.open(out_file, 'wb')
        f_out.writelines(f_in)
	f_out.close()
	f_in.close()
#	print(out_file+" Zipped.")
	File_delete(source_file)



def File_delete(source_file):
    os.remove(source_file)     

    
    
def main():
    database=raw_input("Enter Database name:\n")
    option =raw_input("For "+",".join(ignoreTableList)+" Day Wise Dump Press 1:\n For Whole Database Dump Without "+",".join(ignoreTableList)+" Tables Press 2:\n")
    if int (option)== 1:
        sdate_entry = str(raw_input('Enter Start date in YYYY-MM-DD format:\n'))
        syear, smonth, sday = map(int, sdate_entry.split('-'))
        startDate=date(syear,smonth,sday);

        edate_entry = str(raw_input('Enter End date in YYYY-MM-DD format:\n'))
        eyear, emonth, eday = map(int, edate_entry.split('-'))
        endDate = date(eyear, emonth, eday)

        location_entry =raw_input('Enter Location:\n')
        for tableName in ignoreTableList:
            DateWiseDump(startDate,endDate,tableName,location_entry,database);
    if int (option)==2:
        file_Name =raw_input('Enter File Name:\n')
        DataBaseDump(file_Name,ignoreTableList,database);
        


if __name__== "__main__":
  main()


 








