#!/usr/bin/env python


import platform, datetime, commands,rpyc, shlex, os
import subprocess as sp
#from os.path import splitext, exists

PORT = 18861


class IchabodService(rpyc.SlaveService):
    ALIASES=['ichabod', 'node']

    def on_connect(self):
        # code that runs when a connection is created
        # (to init the serivce, if needed)
        print "Connected. Greetings user!"
        pass

    def on_disconnect(self):
        # code that runs when the connection has already closed
        # (to finalize the service, if needed)
        print "Disconnecting. Goodbye!"
        pass

    def exposed_get_node_info(self):
        uname = platform.uname() # returns tuple: 
        #('Linux', 'F16', '3.2.1-3.fc16.x86_64', '#1 SMP Mon Jan 23 15:36:17 UTC 2012', 'x86_64', 'x86_64')
        arch = platform.architecture()
        python = platform.python_version()
        pythonImp = platform.python_implementation()
        
        info = [arch,uname, python, pythonImp]
        
        return info
    
    def exposed_get_load(self):
        #load = commands.getoutput("w | grep load")
        load = os.getloadavg()
        print 'The load of this host is: ' + str(load)
        return load
        
    def exposed_get_hosts(self):
        f = open('../config/hosts', 'r')
        hosts = []
        for host in f:
            host = host.strip()
            hosts.append(host)
        f.close()
        print "Hosts list loaded into memory: " + str(hosts)
        return hosts
        
    def exposed_get_loads(self):
        hosts = self.exposed_get_hosts()
        loads = []
        for host in hosts:
            connection = shlex.split(host)
            print 'getting host: ' + str(connection)
            c = rpyc.connect(connection[0], int(connection[1]))
            print 'getting load...'
            l = c.root.exposed_get_load()
            print l
            info = [connection[0], connection[1], l]
            loads.append(info)
            c.close()
        print "Loads of all nodes retrieved: " + str(loads)
        return loads
        
    def exposed_upload_job(self, runFile): #argument runFile is literally the string name of the file, assumed to be in the /outbound directory
        hosts = self.exposed_get_loads()
        
        #Prelimenary ~ Parse verify file name
        #make sure argument 'runFile' is a string
        if type(runFile) != str:
            print 'runFile not a string!'
            print 'Trying to convert...'
            runFile = str(runFile)
        else:
            print 'Argument is a string, looks good.'
        
        #STEP 1 - Find host with lowest load average.
        print 'Finding lowest host....'
        lowest = 999999 #arbitrarily high default value so first load avg will be acccepted
        lowest_host = []
        for load in hosts:
            load2 = load[2]
            print 'current node being examined is: ' + str(load)
            if load2[1] < lowest:
                lowest = load2[1]
                lowest_host = load
        print "Lowest node appears to be: " + str(lowest_host)
        
        #STEP 2 - Create classical connection and make use of upload() method
        #         exposed by SlaveService to send job file to the chosen node.
        classic = rpyc.classic.connect(lowest_host[0], port=18812)
        print "Classic connection established: ", classic.root
        print 'Getting ready to upload....'
        # For this to work we are assuming relative path, meaning that the server
        # i.e. this command is being issued from the bin/ directory of the program
        # since absolute paths can be messier and we can control our relative paths
        # much easier using startup scripts instead
        localpath = '../outbound/'+runFile
        
        service = rpyc.connect(lowest_host[0], lowest_host[1])
        #make sure there aren't any files on the host node's end with the same filename
        remoteFile = service.exposed_verify_inbound(runFile)
        
        remotepath = '../inbound/' + str(remoteFile)
        
        rpyc.classic.upload(classic, localpath, remotepath)
        print "File uploaded."
        
        service.exposed_run(remotepath)
        print "File run remotely."

    
    def exposed_verify_inbound(self, inName):
        # When uploading a file to another node, we need to make sure there are no
        # other files that have the same name or that were previously sent over. 
        # This is helpfull for book keeping so we can match incoming and outgoing 
        # files via timestamps that may have been sent over from other nodes.
        # In the future we will apply better methods (unique node hashes) for
        # taging files.
    
        #PART2 ~ Create incoming file
        #create incoming file of type 'filename_inbound_year-month-day.txt'
        now = datetime.datetime.now()
        inName = name + '_inbound_' + now.strftime("%Y-%m-%d") + '.txt'
        #test for duplicate file names (same job re-submitted multiple times)
        if exists(inName) == True: #add hour and minute if another version exists
            inName = name + '_inbound_' + now.strftime("%Y-%m-%d_%H:%M") + '.txt'
        if exists(inName) == True: #add precision down to second if previous version still exists
            inName = name + '_inbound_' + now.strftime("%Y-%m-%d_%H:%M.") + str(now.second) + '.txt'
        if exists(inName) == True: #add microsecond precision if a previous version STILL exists
            inName = name + '_inbound_' + now.strftime("%Y-%m-%d_%H:%M.") + str(now.second)+ '.' + str(now.microsecond) + '.txt'
        print 'Inbound file name is: ' + inName
        
        return inName  
    
    """
    Basic run funtion which executes a program via shell script or python file.
    Later improvements will be to add date/time executed either in the file itself
    or append it to the file name 
    """
    def exposed_run(self, runFile):
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
    
    """
    ###############~~~~~~RabbitMQ Job/Queue Submission~~~~~~####################
    """
    import pika
    
    # STEP1 - Create the queue
    def exposed_create_queue(self, name):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
    
        if type(name) != str:
            name = str(name)
        channel.queue_declare(queue=name)
    
        print 'Queue ' + name + ' created.'
        queue1={'name':name,'connection':connection, 'channel':channel}
        return queue1
        
    #STEP2 - Add a job-filename
    def exposed_submit_job(self, queue2, runFile):
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
    def exposed_run_job(self, queue3):
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
    def exposed_callback(self, ch, method, properties, body):
        print '[x] Received %r' % (body,)
        print 'running job'
        sp.run(body) # ironically this in turn calls my run function to execute the file
        print 'job done'
    """
    ############################################################################
    """
    

def change_global(port):
    global PORT
    PORT = port

if __name__ == "__main__":
        from rpyc.utils.server import ThreadedServer
        p = raw_input("Enter port number: ") #usually 18811
        t = ThreadedServer(IchabodService, port = int(p))
        print 'Ichabod service started on port ' + p
        change_global(int(p))
        print 'Global set to ' + str(PORT)
        t.start()
        print 'test'
    
    
