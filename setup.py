#!/usr/bin/env python
"""
    Jean-Lou Dupont's AWS scripts
    
    Created on 2012-01-19
    @author: jldupont
"""
__author__  ="Jean-Lou Dupont"
__version__ ="0.2.1"


from distutils.core import setup
from setuptools import find_packages


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
                     ],
      package_data = {
                      '':[ "*.gif", "*.png", "*.jpg" ],
                      },
      include_package_data=True,                      
      zip_safe=False
      )
