#!/usr/bin/env python

import socket
import subprocess
import os
import sys
import threading

s1,s2 = socket.socketpair()

p1 = subprocess.Popen(['socat', '-', 'FD:' + str(s1.fileno())])

env = dict(os.environ)
env['NODE_CHANNEL_FD'] = str(s2.fileno())

p2 = subprocess.Popen(['node', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'change_attr.js')] + sys.argv[1:], env=env, stdin=subprocess.PIPE, stdout=2)
p2.stdin.close()

#s1.close()
#s2.close()

done = False

class WaitThread(threading.Thread):
    def run(self):
        p1.wait()
        if not done: p2.kill()

thread = WaitThread()
thread.start()

p2.wait()
done = True
p1.kill()
