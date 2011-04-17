#!/usr/bin/python

import sys
import threading
import socket
import random
import time

from string import index;
from math import log;
from copy import deepcopy;

def formalize(ip):
    tokens = ip.split(".")
    assert len(tokens)==4
    
    for i in xrange(len(tokens)):
        assert len(tokens[i])<=3 and len(tokens[i])>0
        for j in xrange(3-len(tokens[i])):
            tokens[i] = "0"+tokens[i]
                    
    return ".".join(tokens)

def getExpression(ip):
    ip = formalize(ip)
    return "".join(ip.split("."))
    
if __name__ == "__main__":
    print formalize("12.52.126.25")
    print getExpression("12.52.421.54")
    
    print abs(compare(getExpression("12.52.421.54"), getExpression("12.52.421.54")))