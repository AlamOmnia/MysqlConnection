#!/usr/bin/env python
import ConfigParser
import os
import time


config = ConfigParser.ConfigParser()
#config.read("/mysql-test/include/my.cnf")
username = 'root'
password = 'Takay1#$ane'
hostname = 'localhost'

filestamp = time.strftime('%Y-%m-%d')

# Get a list of databases with :
database_list_command="mysql -u %s -p%s -h %s --silent -N -e 'show databases'" % (username, password, hostname)

database='purple'
filename = "D:/%s-%s.sql" % (database, filestamp)
os.popen("mysqldump -u %s -p%s -h %s -e --opt -c %s | gzip -c > %s.gz" % (username, password, hostname, database, filename))