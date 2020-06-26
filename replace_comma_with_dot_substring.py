#!/usr/bin/python
# coding=utf-8
import os
import sys
import re

csvfilePath = sys.argv[1]
fileArray = os.listdir(csvfilePath)
cwd = os.getcwd()

for filename in fileArray:
    if filename.endswith(".csv"):
        abs_path = cwd+'/'+csvfilePath
        new_file_fd = os.open(cwd+'/'+filename+'_new.csv', os.O_RDWR|os.O_CREAT)
        with os.fdopen(new_file_fd,'w') as new_file:
            with open(abs_path+filename) as old_file:
                print("Reading file: "+abs_path+filename)
                print("Writing file: "+cwd+'/'+filename+'_new.csv')
                for count,line in enumerate(old_file):

                    begin_qoute = line.find("\"")
                    while begin_qoute is not -1:
                        end_qoute = line.find("\"", begin_qoute+1)
                        sub_string = line[begin_qoute+1:end_qoute]
                        sub_string_new = sub_string.replace(',','.')
                        line = line.replace(sub_string, sub_string_new)
                        begin_qoute = line.find("\"", end_qoute+1)
                    new_file.write(line)


print("Done")
