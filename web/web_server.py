#!/usr/bin/env python

from flask import Flask
from flask import render_template
import rpyc

app = Flask(__name__)

@app.route('/',methods=['GET'])
def hello_world():
    c = rpyc.connect('localhost',18861)
    info = c.root.exposed_get_node_info()
    load = c.root.exposed_get_load()
    c.close()
    return render_template('index.html', info=info, load=load)

@app.route('/node_info/')

@app.route('/submit_job/')

@app.route('/network_load/')

@app.route('/projects/')
def projects():
    return 'The project page'

@app.route('/about')
def about():
    return 'The about page'

if __name__ == '__main__':
    app.debug = True
    app.run(host='192.168.1.101')
    
