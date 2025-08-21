#!/home/xs332906/hopnic.org/public_html/new_venv/bin/python
# -*- coding: utf-8 -*-
print ("Content-Type: text/html\n\n")
from wsgiref.handlers import CGIHandler
from run import app
CGIHandler().run(app)