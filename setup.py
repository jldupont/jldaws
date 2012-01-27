#!/usr/bin/env python
"""
    Jean-Lou Dupont's AWS scripts
    
    Created on 2012-01-19
    @author: jldupont
"""
__author__  ="Jean-Lou Dupont"
__version__ ="0.2.4"


from distutils.core import setup
from setuptools import find_packages

DESC="""
Overview
--------

This package contains a collection of Amazon Web Service related scripts e.g.

* jlds3upload : automated file upload to S3
* jldrxsqs : receive from an SQS queue to JSON/string stdout
* jldtxsqs : transmit on SQS JSON/string from stdin

The philosophy behind these scripts is:

* simplicity  : each script only does 1 thing
* reporting   : status & error reporting through stderr and (optionally) JSON stdout
* data driven : object in from src ==> object out to dst
* data flow   : piping through stdin / stdout    

Scripts can easily be "composed" through standard Linux piping :

    jldrxsqs @config1.txt | some_other_command | jldtxsqs @config2.txt

The data format used is JSON.

Configuration
-------------

Can be performed through options on the command line or using a file (use a leading `@`).
"""


setup(name=         'jldaws',
      version=      __version__,
      description=  'Collection of Amazon AWS related scripts',
      author=       __author__,
      author_email= 'jl@jldupont.com',
      url=          'http://www.systemical.com/doc/opensource/jldaws',
      package_dir=  {'': "src",},
      packages=     find_packages("src"),
      scripts=      ['src/scripts/jldexec',
                     'src/scripts/jlds3notify',
                     'src/scripts/jldtxsqs',
                     'src/scripts/jldrxsqs',     
                     'src/scripts/jlds3upload',
                     ],
      package_data = {
                      '':[ "*.gif", "*.png", "*.jpg" ],
                      },
      include_package_data=True,                      
      zip_safe=False,
      long_description=DESC
      )
