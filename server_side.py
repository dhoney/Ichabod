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

def create_queue(name):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
                 pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))  
    channel = connection.channel()

    if type(name) != str:
        name = str(name)
    channel.queue_declare(queue=name)

    print 'Queue ' + name + ' created.'
    return
    
def submit_job(queue, runFile):
    if type(name) != str:
        name = str(name)
    elif type(queue) != str:
        queue = str(queue)
        
    channel.basic_publish(exchange='', routing_key=queue, body=runFile)
    print ' [x] Submitted job file: ' + runFile + ' ,to queue: ' + queue
    connection.close()
    return
    
def run_job(name):
    if type(name) != str:
        name = str(name)
    #check queue exists just in case
    channel.queue_declare(queue=name)
    
    channel.basic_consume(callback, queue=name, no_ack=True)
    print "[*] Waiting for jobs. Press CTRL+C to exit."
    channel.start_consuming()

    return

def callback(ch, method, properties, body):
    print '[x] Received %r' % (body,)
    print 'running job'
    run(body)
    print 'job done'
    
    