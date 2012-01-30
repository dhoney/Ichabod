#!/usr/bin/env python


import platform


def get_node_info():
    uname = platform.uname() # returns tuple: 
    #('Linux', 'F16', '3.2.1-3.fc16.x86_64', '#1 SMP Mon Jan 23 15:36:17 UTC 2012', 'x86_64', 'x86_64')
    arch = platform.architecture()
    python = platform.python_version()
    pythonImp = platform.python_implementation()
    
    info = [arch,uname, python, pythonImp]
    
    return info
    
    