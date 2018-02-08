# -*- coding: utf-8 -*-
import datetime, time
from bson.objectid import ObjectId


def str2datetime(val, fmt='%Y-%m-%dT%H:%M:%SZ'):
    return datetime.datetime.strptime(val, fmt)

def datetime2objectid(val, fmt='%Y-%m-%dT%H:%M:%SZ'):
    dt = str2datetime(val, fmt)
    timestamp = time.mktime(dt.timetuple())
    oidstr = hex(long(timestamp)).rstrip("L").lstrip("0x") + '0000000000000000'
    return ObjectId(oidstr)
