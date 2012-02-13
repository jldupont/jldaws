#!/usr/bin/env python
"""
    Jean-Lou Dupont's AWS scripts
    
    Created on 2012-01-19
    @author: jldupont
"""
__author__  ="Jean-Lou Dupont"
__version__ ="0.2.8"


from distutils.core import setup
from setuptools import find_packages

DESC="""
Overview
--------

This package contains a collection of Amazon Web Service related scripts e.g.

* jlds3upload : automated file upload to S3
* jlds3download : automated file download from S3
* jldrxsqs : receive from an SQS queue to JSON/string stdout (with optional stdin trigger)
* jldtxsqs : transmit on SQS JSON/string from stdin
* jldleader: distributed leader election protocol manager
* jlds3up  : simple file upload to s3 with functionality to delete 'old' files

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
                     'src/scripts/jlds3download',
                     'src/scripts/jldleader',
                     'src/scripts/jlds3up',
                     ],
      zip_safe=False
      ,long_description=DESC
      ,install_requires=["pyfnc >= 0.1.2"]
      )
