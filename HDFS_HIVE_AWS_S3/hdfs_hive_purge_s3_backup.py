#!/usr/bin/python
import datetime
import requests
import sys
import json
import os
import logging, logging.handlers
from subprocess import call
from pyhive import hive
import pdb
import argparse

now = datetime.datetime.now()
today = now.strftime("%Y-%m-%d")
print "TODAY = " + today
logfile = "/var/jenkins/workspace/HDFS_Purge/hdfs_purge_log_" + today + ".log"
print "LOGFILE = " + logfile
jsonFile = "/var/jenkins/workspace/HDFS_Purge/data_" + today + ".json"
print "JSONFILE = " + jsonFile

logging.basicConfig(filename=logfile,level=logging.DEBUG, format='%(asctime)s %(message)s')
logging.debug('DEBUG: Start process')

def create_file():
    f = open("initial.txt", 'w')
    f.write('hello\n')
    f.close()

def awsCreateFolder(aws_create_folder):
    print datetime.datetime.utcnow()
    ret = call(aws_create_folder, shell=True)
    print datetime.datetime.utcnow()
    if ret != 0:
        if ret < 0:
            print "Killed by signal", -ret
        else:
            print "Command failed with return code", ret
    else:
        print "Success of aws create folder!"

def hadoopDistcp(hadoopCopyS3):
    print datetime.datetime.utcnow()
    ret = call(hadoopCopyS3, shell=True)
    print datetime.datetime.utcnow()
    if ret != 0:
        if ret < 0:
            print "Killed by signal", -ret
        else:
            print "Command failed with return code", ret
    else:
        print "Success of hadoop distcp call!"
    logging.info('INFO: This folder was created in AWS S3 after being copied from hdfs: %s', hadoopCopyS3)

def hdfsDelete(hadoopDelete):
    print datetime.datetime.utcnow()
    print "HadoopDelete = " + hadoopDelete
    ret = call(hadoopDelete, shell=True)
    print datetime.datetime.utcnow()
    if ret != 0:
        if ret < 0:
            print "Killed by signal", -ret
        else:
            print "Command failed with return code", ret
    else:
        print "Success of delete file from hdfs"
    logging.info('INFO: This folder was deleted from hdfs using this command =  %s', hadoopDelete)

def hiveDelete(cnxn, dobj):
    logging.info('INFO: Inside hiveDelete')
    if dobj is None:
        print "don't delete any hive table"
        logging.info('INFO: HiveTables set as null in API, no delete')
    else:
        for a in dobj:
            logging.info('INFO: Inside hiveDelete for')
            hivetable = str(a)
            print "hivetable = " + hivetable
            if hivetable.find('-') != -1:
                logging.info('INFO: This table contains invalid character -. Do not delete.  %s', hivetable)
            else:
                hiveDelete = "drop table if exists " + hivetable
                print "hiveDelete = " , hiveDelete
                cursor = cnxn.cursor()
                cursor.execute(hiveDelete)
                print cursor.fetchall()
                logging.info('INFO: This table was deleted from hive using this command =  %s', hiveDelete)
        logging.info('INFO: End of for')


parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description='''
Purges HDFS and Hive Data

''')
parser.add_argument('--which', default="x", help='x or y')
parser.add_argument('--stack', default="a", choices=['a', 'b'], help='Which stack, a or b')
parser.add_argument('--profile', default="default", help='profile to use, aws cli ~/.aws/credentials.')
parser.add_argument('--region', default="us-east-1", help='AWS regions to user.')
parser.add_argument('--doit', default=False, action='store_true', help='Will perform delete')

args = parser.parse_args()

awsProfile = "--profile " + args.profile
print "AWS Profile " + awsProfile
if args.stack == 'a':
    url = ''
else:
    url = ''
# Prod URL
headers = {}

logging.debug('DEBUG: Establishing hive connection')
#hive connection - active will always be x unlike hdfs which is HA and switches active
active = args.which
print " Active " + active
try:
    cnxn = hive.Connection(host="HIVE_CONNECTION", port=x, username="username")
except Exception, ex:
    print ex
    sys.exit(1)

logging.debug('DEBUG: Establishing connection to API')
# get request to provided api
r = requests.get(url, headers=headers, verify=False)
if r.status_code != 200:
    print "ERROR: Purge URL Failed - is stack still up?"
    print r.text
    sys.exit(0)
# built in json decoder
data = r.json()
with open(jsonFile, 'a') as f:
    json.dump(data, f)

# set datanode path depending on which is active
if active == "x":
    dataNode = "hdfs:/x:8020"
elif active == "y":
    dataNode = "hdfs://y:8020"
else:
    print "ERROR:" + active + " invalid, expected x or y"
    sys.exit(-1)

print "dataNode = " + dataNode

# S3 destination used by hadoop distcp:
s3dest_hdfs = "s3a://S3_BUCKET/"
# s3 destination used for aws folder creation
s3dest = "s3://S3_BUCKET"

#counters to keep track of s3 copies and hdfs and hive deletions
s3CopyCounter = 0
hdfsDeleteCounter = 0
hiveDeleteCounter = 0

logging.debug('DEBUG: Starting walk through of JSON returned by API')
# logic to extract hdfs and hive paths from api
# for x in range(len(data)):
for dobj in data:
    for y in dobj:
        if y == 'SourceName':
            sourcename = str(dobj['SourceName'])
            print "SOURCENAME " + sourcename
            s3days = str(dobj['S3Days'])
            print "S3 Days " + s3days
            glacierDays = str(dobj['GlacierDays'])
            print "Glacier Days " + glacierDays
            HDFSPaths = str(dobj['HdfsPaths'])
            print "HDFS PATHS:" + HDFSPaths
            print "Sourcename" + sourcename
            print "ToBak value" + str(dobj['ToBak'])
            print "HIVE TABLES ==== "
            print dobj['HiveTables']
            print str(dobj['HiveTables'])
            for currHdfs in dobj['HdfsPaths']:
                print "Current HdfsPath"
                print currHdfs
                print "Current ToBak value" + str(dobj['ToBak'])
                if currHdfs is None:
                    print "------------------ IN currHdfs = NONE BLOCK ------------------"
                    print "Nothing to delete. ToBak value doesn't matter"
                if currHdfs is not None:
                    print "------------------ IN currHdfs is NOT NONE BLOCK ------------------"
                    if (dobj['ToBak'] is True) or (str(dobj['ToBak']) == "True") or (
                       str(dobj['ToBak']) == "true"):
                        print "--------------- IN TRUE BLOCK ---------------"
                        print "ToBak = True"
                        print "Move to cold storage then delete from hdfs and hive"
                        logging.info('INFO: ToBak = True so move to cold storage then delete from HDFS and Hive')
                        s3folder = s3dest + '/' + s3days + '/' + glacierDays + currHdfs + '/'
                        print s3folder
                        s3folder_hdfs = s3dest_hdfs + '/' + s3days + '/' + glacierDays + currHdfs + '/'
                        print "s3folder_hdfs = ", s3folder_hdfs
                        print "create dummy file hello"
                        #create dummy file to create new folder in buckets with due to aws s3 create new folder syntax
                        try:
                            create_file()
                            logging.info('INFO: dummy file to initialize folder in S3 was created (initial.txt)')
                        except Exception as e:
                            print e
                            print "creation of dummy file (initial.txt) for S3 bucket failed"
                            logging.error('ERROR: Creation of dummy file (initial.txt) for S3 bucket failed')
                        else:  # create folder in s3 bucket that will hold hdfs files; put helloWorld file to create initially
                            try:
                                aws_create_folder = "aws s3 mv initial.txt " + s3folder + "initial.txt " + awsProfile
                                print "create aws folder:" + aws_create_folder
                                awsCreateFolder(aws_create_folder)
                                logging.info('INFO: This folder was created in AWS S3: %s', s3folder)
                            except Exception as e:
                                print e
                                print "aws command to create folder in S3 failed"
                                logging.error('ERROR: This folder failed to create in AWS S3: %s', s3folder)
                        # try to copy hdfs path files to S3
                        try:
                            copyFile = dataNode + currHdfs
                            hadoopCopyS3 = "hadoop distcp -update " + copyFile + " " + s3folder_hdfs
                            print "hadoopCopyS3 " + hadoopCopyS3
                            print datetime.datetime.utcnow()
                            testPath = "hadoop fs -test -e " + copyFile
                            print "testPath = " + testPath
                            #check if hdfs file path exists
                            ret = call(testPath, shell=True)
                            print "ret = ", ret
                            #if path exists, copy to S3; else don't copy to S3
                            if ret == 0:
                                hadoopDistcp(hadoopCopyS3)
                                s3CopyCounter = s3CopyCounter + 1
                                print "s3CopyCounter (# folders that copied to S3 so far) ", s3CopyCounter
                                logging.info('INFO: s3CopyCounter (# folders that copied to S3 so far) = %s', s3CopyCounter)
                            else:
                                print "Don't copy HDFS Path to S3 since path does not exist"
                                logging.error('ERROR: This folder failed to copy to AWS S3 since path does not exist = : %s', copyFile)
                        except Exception as e:
                            print e  # catch exceptions if it copy fails
                            print "Copy from HDFS to S3 Failed"
                            logging.error('ERROR: This folder failed to copy to AWS S3 through this command = : %s', hadoopCopyS3)
                        else:
                            print "----- IN ELSE BLOCK -----"
                            # if copy from hdfs to s3 succeeded, delete from hdfs and delete the hive table
                            print "Deleting HDFS directory after successful copy to S3"
                            try:
                                hadoopDelete = "hadoop fs -rm -r " + copyFile + "/"
                                if args.doit:
                                    print "args.doit = True so delete from hdfs"
                                    testPath = "hadoop fs -test -e " + copyFile
                                    print "testPath = " + testPath
                                    #check if hdfs file path exists
                                    ret = call(testPath, shell=True)
                                    print "ret = ", ret
                                    #if path exits, delete from hdfs; else don't delete from HDFS
                                    if ret == 0:
                                        hdfsDelete(hadoopDelete)
                                        hdfsDeleteCounter = hdfsDeleteCounter + 1
                                        print "hdfsDeleteCounter (# folders that were deleted from HDFS so far) ", hdfsDeleteCounter
                                        logging.info('INFO: hdfsDeleteCounter (# folders that were deleted from HDFS so far) = %s', hdfsDeleteCounter)
                                    else:
                                        print "Don't delete HDFS Path since path does not exist"
                                        logging.error('ERROR: This hdfs folder failed to delete since path does not exist = : %s', copyFile)
                                else:
                                    print "args.doit = False so just print and don't delete from hdfs"
                                    print hadoopDelete
                                    logging.info('INFO: We are in test mode since the doit flag was set to false. Will not delete from HDFS')
                                logging.info('INFO: try block with hdfs delete finished')
                            except Exception as e:
                                print e
                                print "Deleting from HDFS after copying to S3 failed"
                                logging.error('ERROR: This folder failed to delete from HDFS through this command = : %s', hadoopDelete)
                            else:# delete hive table if deleting hdfs directory was successful
                                logging.info('INFO: in else block since try was successful')
                                if args.doit:
                                    logging.info('INFO: since args.doit is true, delete from hive')
                                    print "args.doit = True so delete from hive"
                                    print dobj['HiveTables']
                                    hiveDelete(cnxn, dobj['HiveTables'])
                                    hiveDeleteCounter = hiveDeleteCounter + 1
                                    print "hiveDeleteCounter (# tables that were deleted from HIVE so far) ", hiveDeleteCounter
                                    logging.info('INFO: hiveDeleteCounter (# tables that were deleted from HIVE so far) = %s', hiveDeleteCounter)
                                else:
                                    print "printing hive delete...not actually deleting"
                                    logging.info('INFO: We are in test mode since the doit flag was set to false. Will not delete from Hive')  
                                logging.info('INFO: end of else block with hdfs delete')         
                    if (dobj['ToBak'] is False) or (str(dobj['ToBak']) == "False") or (
                       str(dobj['ToBak']) == "false"):
                        print "--------------- IN FALSE BLOCK ---------------"
                        #print s3folder_hdfs
                        print "Just delete from HDFS and HIVE...no cold storage"
                        logging.info('INFO: ToBak = False so delete from hdfs and hive...no cold storage')
                        copyFile = dataNode + currHdfs
                        #delete hadoop directory
                        try:
                            hadoopDelete = "hadoop fs -rm -r " + copyFile
                            if args.doit:
                                print "args.doit = True so delete from hdfs"
                                testPath = "hadoop fs -test -e " + copyFile
                                print "testPath = " + testPath
                                #check if hdfs path exists
                                ret = call(testPath, shell=True)
                                print "ret = ", ret
                                #if path exists, delete from hdfs; else don't delete from hdfs
                                if ret == 0:
                                    hdfsDelete(hadoopDelete)
                                    hdfsDeleteCounter = hdfsDeleteCounter + 1
                                    print "hdfsDeleteCounter (# folders that were deleted from HDFS so far) ", hdfsDeleteCounter
                                    logging.info('INFO: hdfsDeleteCounter (# folders that were deleted from HDFS so far) = %s', hdfsDeleteCounter)
                                else:
                                    print "Don't delete HDFS Path since path does not exist"
                                    logging.error('ERROR: This hdfs folder failed to delete since path does not exist = : %s', copyFile)
                            else:
                                print "hadoopDelete " + hadoopDelete
                                logging.info('INFO: We are in test mode since the doit flag was set to false. Will not delete from HDFS')
                        except Exception as e:
                            print e
                        else:
                            #if hdfs path delete was successful, move on to delete hive
                            print "----- IN ELSE BLOCK -----"
                            #delete hive table
                            if args.doit:
                                print "args.doit = True so delete from hive"
                                hiveDelete(cnxn, dobj['HiveTables'])
                                hiveDeleteCounter = hiveDeleteCounter + 1
                                print "hiveDeleteCounter (# tables that were deleted from HIVE so far) ", hiveDeleteCounter
                                logging.info('INFO: hiveDeleteCounter (# tables that were deleted from HIVE so far) = %s', hiveDeleteCounter)
                            else:
                                print "printing hive delete...not actually deleting"
                                logging.info('INFO: We are in test mode since the doit flag was set to false. Will not delete from Hive')
print "FINAL s3CopyCounter (# folders that copied to S3) ", s3CopyCounter
logging.info('INFO: FINAL s3CopyCounter (# folders that copied to S3 total) = %s', s3CopyCounter)
print "FINAL hdfsDeleteCounter (# folders that were deleted from HDFS) ", hdfsDeleteCounter
logging.info('INFO: FINAL hdfsDeleteCounter (# folders that were deleted from HDFS total) = %s', hdfsDeleteCounter)
print "FINAL hiveDeleteCounter (# tables that were deleted from HIVE) ", hiveDeleteCounter
logging.info('INFO: FINAL hiveDeleteCounter (# tables that were deleted from HIVE total) = %s', hiveDeleteCounter)
logging.info('')
logging.info('DEBUG: End of Process')
logging.info('')
logging.info('')
logging.shutdown()
#copy log file to s3
try:
    s3copy_log = "aws s3 cp " + logfile + " s3://S3_BUCKET/Logs/"
    print "Copy log for HDFS_Purge script to S3 using this command: " + s3copy_log
    call(s3copy_log, shell=True)
except Exception as e:
    print e
    print "aws command to copy log to s3 failed"
#copy json file containing api return for today to s3
try:
    s3copy_json = "aws s3 cp " + jsonFile + " s3://S3_BUCKET/Logs/"
    print "Copy json return from API to S3 using this command: " + s3copy_json
    call(s3copy_json, shell=True)
except Exception as e:
    print e
    print "aws command to copy json file to s3 failed"
