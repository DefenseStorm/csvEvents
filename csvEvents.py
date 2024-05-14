#!/usr/bin/env python3

import sys,os,getopt
import traceback
import os
import fcntl
import json
import requests
from requests.auth import HTTPBasicAuth
import time
import re
from datetime import datetime, timedelta
import base64
from os import listdir
from os.path import isfile, join
import csv
import pytz

sys.path.insert(0, './ds-integration')
from DefenseStorm import DefenseStorm

from html.parser import HTMLParser

class integration(object):

    def checkDirectory(self):
        try:
            file_list = [f for f in listdir(self.watch_dir) if isfile(join(self.watch_dir, f))]
        except Exception as e:
            self.ds.logger.error("Failed to read directory: %s" %self.watch_dir)
            self.ds.logger.error("Exception {0}".format(str(e)))
            self.ds.logger.error("%s" %(traceback.format_exc().replace('\n',';')))
            return None

        for file_name in file_list:
            data_type = None
            self.ds.logger.info("Checking file: %s" %(file_name))
            data_type = None
            for file_type in self.event_mappings.keys():
                if file_type in file_name:
                    self.ds.logger.info("Found file type: %s" %(file_type))
                    data_type = file_type

            if data_type == None:
                self.ds.logger.info("No matching type for file. Skipping: %s" %(file_name))
                continue
            '''
            event_list = self.readCSVFile(self.watch_dir + '/' + file_name)
            if event_list == None:
                self.ds.logger.error('Failed to read csv data for file: %s' %file_name)
                continue

            if data_type == None:
                self.ds.logger.error('invalid data type. Skipping: %s' %data_type)
                continue
            '''
            for event in self.readCSVFile(self.watch_dir + '/' + file_name):
                if self.timezone_string != None:
                    for field in self.timezone_fields:
                        if field in event.keys() and event[field] != '':
                            new_time = pytz.timezone(self.timezone_string).localize(datetime.strptime(event[field], '%Y-%m-%dT%H:%M:%S'))
                            event[field] = new_time.isoformat()
                self.ds.writeJSONEvent(event, JSON_field_mappings = self.JSON_field_mappings, app_name = data_type)

            self.ds.logger.info('Backing up file: %s to directory %s' %(file_name, self.backup_dir))
            try:
                os.rename(self.watch_dir + '/' + file_name, self.backup_dir + '/' + file_name)
            except Exception as e:
                self.ds.logger.error("Failed to backup file: %s" %file_name)
                self.ds.logger.error("Exception {0}".format(str(e)))
                continue

        return True

    def readCSVFile(self, csvFilePath):
        data = []
        try:
            with open(csvFilePath, encoding='utf-8') as csvf:
                csvReader = csv.DictReader(csvf)
                for row in csvReader:
                    yield row
                '''
                for row in csvReader:
                    data.append(row)
                '''
        except Exception as e:
            self.ds.logger.error("Failed to read CSV file: %s" %csvFilePath)
            self.ds.logger.error("Exception {0}".format(str(e)))
            self.ds.logger.error("%s" %(traceback.format_exc().replace('\n',';')))
            return None
        return

    def readMappingsFile(self, filePath):
        data = []
        try:
            with open(filePath, encoding='utf-8') as mFile:
                data = json.load(mFile)
        except Exception as e:
            self.ds.logger.error("Failed to read Mappings file: %s" %filePath)
            self.ds.logger.error("Exception {0}".format(str(e)))
            self.ds.logger.error("%s" %(traceback.format_exc().replace('\n',';')))
            return None
        return data

    def csv_main(self): 

        self.watch_dir = self.ds.config_get('csv', 'watch_dir')
        self.backup_dir = self.ds.config_get('csv', 'backup_dir')
        self.state_dir = self.ds.config_get('csv', 'state_dir')
        self.field_mappings_file = self.ds.config_get('csv', 'field_mappings_file')
        self.event_mappings_file = self.ds.config_get('csv', 'event_mappings_file')
        timezone_string = self.ds.config_get('csv', 'timezone_string')
        self.timezone_fields = self.ds.config_get('csv', 'timezone_fields').split(',')
        if timezone_string in pytz.all_timezones:
            self.timezone_string = timezone_string
            self.ds.logger.info('Found timezone: %s' %self.timezone_string)
        else:
            self.ds.logger.warning('Invalid timezone string detected "%s".  Continuing without setting timezone' %timezone_string)
            self.timezone_string = None

        if not os.path.isdir(self.watch_dir):
            self.ds.logger.error('Directory does not exist: %s' %self.watch_dir)
            return

        if not os.path.isdir(self.backup_dir):
            self.ds.logger.error('Directory does not exist: %s' %self.backup_dir)
            return

        if not os.path.isfile(self.field_mappings_file):
            self.ds.logger.error('Field Mappings file does not exist: %s' %self.mappings_file)
            return
        if not os.path.isfile(self.event_mappings_file):
            self.ds.logger.error('Event Mappings file does not exist: %s' %self.mappings_file)
            return

        self.JSON_field_mappings = self.readMappingsFile(self.field_mappings_file)
        self.event_mappings = self.readMappingsFile(self.event_mappings_file)

        if not self.checkDirectory():
            self.ds.logger.error('Failed in data run')

        self.ds.log('INFO', "Done With Run")

        return

    def run(self):
        try:
            pid_file = self.ds.config_get('csv', 'pid_file')
            fp = open(pid_file, 'w')
            try:
                fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                self.ds.log('ERROR', "An instance of csv is already running")
                # another instance is running
                sys.exit(0)
            self.csv_main()
        except Exception as e:
            traceback.print_exc()
            self.ds.logger.error("Exception {0}".format(str(e)))
            return
    
    def usage(self):
        print
        print(os.path.basename(__file__))
        print
        print('  No Options: Run a normal cycle')
        print
        print('  -t    Testing mode.  Do all the work but do not send events to GRID via ')
        print('        syslog Local7.  Instead write the events to file \'output.TIMESTAMP\'')
        print('        in the current directory')
        print
        print('  -l    Log to stdout instead of syslog Local6')
        print
        print('  -c <counter>  Starting point for restarting run')
        print
        print('  -m <module>  Moudle to restart at after errors')
        print('               NOTE: Modules run in order of:')
        print('                    architect, dna, secure-now')
        print('               script will continue from specified starting point')
        print
    
    def __init__(self, argv):

        self.testing = False
        self.send_syslog = True
        self.ds = None
        self.restart_counter = None
        self.restart_module = None
    
        try:
            opts, args = getopt.getopt(argv,"htnlc:m:",["datedir="])
        except getopt.GetoptError:
            self.usage()

            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                self.usage()
                sys.exit()
            elif opt in ("-t"):
                self.testing = True
            elif opt in ("-l"):
                self.send_syslog = False
            elif opt in ("-c"):
                self.restart_counter = arg
            elif opt in ("-m"):
                self.restart_module = arg
        if (self.restart_counter == None and self.restart_module != None) or \
            (self.restart_counter != None and self.restart_module == None):
            print()
            print('-c and -m need to be used together')
            print()
            self.usage()
            sys.exit(2)
        try:
            self.ds = DefenseStorm('csvEvents', testing=self.testing, send_syslog = self.send_syslog)
        except Exception as e:
            traceback.print_exc()
            try:
                self.ds.log('ERROR', 'ERROR: ' + str(e))
            except:
                pass


if __name__ == "__main__":
    i = integration(sys.argv[1:]) 
    i.run()
