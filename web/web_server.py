#!/usr/bin/env python

from flask import Flask, request, redirect, url_for, render_template
import rpyc

UPLOAD_FOLDER = '../uploads/'
ALLOWED_EXTENSIONS = set(['py', 'sh'])


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

@app.route('/',methods=['GET', 'POST'])
def node_page():
    if request.method == 'POST':
        return 'posted'
    
    c = rpyc.connect('localhost',18861)
    info = c.root.exposed_get_node_info()
    load = c.root.exposed_get_load()
    return render_template('index.html', info=info, load=load)

"""
@app.route('/node_info/')
@app.route('/submit_job/')
@app.route('/network_load/')
@app.route('/projects/')

def projects():
    return render_template('projects.html')

@app.route('/about')
def about():
    return 'render_template('about.html')
"""

if __name__ == '__main__':
    app.debug = True
    app.run(host='192.168.1.101')
    
