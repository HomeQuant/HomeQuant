#!/usr/bin/env python
#  -*- coding: utf-8 -*-
"""
@ProjectName: HomeQuant
@FileName   : __init__.py.py
@Date       : 2026/2/12 14:32
@Author     : Lumosylva
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
import inspect

from . import request

REQUEST_PAYLOAD = {}

for name, value in inspect.getmembers(request):
    if name.startswith("WebCTP") and name != "WebCTPRequest":
        REQUEST_PAYLOAD[name.replace("WebCTP", "")] = value
