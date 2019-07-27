# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 18:45:05 2019

@author: Tanmay.Sidhu
"""

import simple_bot

from flask import Flask, render_template
from flask_socketio import SocketIO

app=Flask(__name__)
app.config['SECRET_KEY']='secret!'
socketio=SocketIO(app)

if __name__=='__main__':
    socketio.run(app)


from flask_socketio import send, emit
#def handle_my_custom_event(json):
#    emit('my response', json)
def handle_my_custom_event(json):
    emit('my response', json)