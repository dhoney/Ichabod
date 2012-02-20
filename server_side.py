#!/usr/bin/env python


import platform, datetime
import subprocess as sp
from os.path import splitext, exists


def get_node_info():
    uname = platform.uname() # returns tuple: 
    #('Linux', 'F16', '3.2.1-3.fc16.x86_64', '#1 SMP Mon Jan 23 15:36:17 UTC 2012', 'x86_64', 'x86_64')
    arch = platform.architecture()
    python = platform.python_version()
    pythonImp = platform.python_implementation()
    
    info = [arch,uname, python, pythonImp]
    
    return info

"""
Basic run funtion which executes a program via shell script or python file.
Later improvements will be to add date/time executed either in the file itself
or append it to the file name 
"""
def run(runFile):
    #PART1 ~ Parse argument/file name
    #make sure argument is a string
    if type(runFile) != str:
        print 'runFile not a string!'
        runFile = str(runFile)
    else:
        print 'argument is a string'
        
    #get file name and extension
    name, ext = splitext(runFile)
    print 'name is ' + name
    print 'extension is ' + ext
    #test for correct file extensions
    if ext != '.py'and ext != '.sh':
        return 'File not python or shell script!'
    
    #PART2 ~ Create output file
    #create output file of type 'filename_output_year-month-day.txt'
    now = datetime.datetime.now()
    outName = name + '_output_' + now.strftime("%Y-%m-%d") + '.txt'
    #test for duplicate file names (same job re-run multiple times)
    if exists(outName) == True: #add hour and minute if another version exists
        outName = name + '_output_' + now.strftime("%Y-%m-%d_%H:%M") + '.txt'
    if exists(outName) == True: #add precision down to second if previous version still exists
        outName = name + '_output_' + now.strftime("%Y-%m-%d_%H:%M.") + str(now.second) + '.txt'
    if exists(outName) == True: #add microsecond precision if a previous version STILL exists
        outName = name + '_output_' + now.strftime("%Y-%m-%d_%H:%M.") + str(now.second)+ '.' + str(now.microsecond) + '.txt'
    print 'output file name is: ' + outName
    output_f = open(outName, 'w')
    print 'output file created'
    
    #PART3 ~ Execute command on submitted file/job
    #run the actual command
    if ext == '.py':
        proc = sp.Popen(['python', runFile], stdout=output_f, stderr=output_f, shell=False)
    elif ext == '.sh':
        proc = sp.Popen(['sh', runFile], stdout=output_f, stderr=output_f, shell=False)
    
    output_f.close()
    print 'file closed'
    
    #PART4 ~ All Done!
    print 'success!'
    
    return

import pika

# STEP1 - Create the queue
def create_queue(name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    if type(name) != str:
        name = str(name)
    channel.queue_declare(queue=name)

    print 'Queue ' + name + ' created.'
    queue1={'name':name,'connection':connection, 'channel':channel}
    return queue1
    
#STEP2 - Add a job-filename
def submit_job(queue2, runFile):
    if type(runFile) != str:
        runFile = str(runFile)
    elif type(queue2) != dict:
        return "Error: Expecting argument 1 as dictionary type"
        
    queue2['channel'].basic_publish(exchange='', routing_key=queue2['name'], body=runFile)
    print ' [x] Submitted job file: ' + runFile + ' ,to queue: ' + queue2['name']
    queue2['connection'].close()
    
    queue2['job'] = runFile
    return queue2
    
#STEP3 - Initiate queue connection as a 'client' and run job
def run_job(queue3):
    if type(queue3) != dict:
        return "Error: Expecting argument 1 as dictionary type"
        
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='hello')

    print ' [*] Waiting for messages. To exit press CTRL+C'

    channel.basic_consume(callback, queue='hello', no_ack=True)
    channel.start_consuming()

    return

#STEP4 - this is a RabbitMQ function called by 'run_job' to actually execute the job
def callback(ch, method, properties, body):
    print '[x] Received %r' % (body,)
    print 'running job'
    sp.run(body) # ironically this in turn calls my run function to execute the file
    print 'job done'

    
    