#!/bin/sh

echo "Submitting egg to Pypi"
python setup.py sdist --formats=zip,gztar upload

VERSION=`cat latest`

echo "Tagging & submitting to Pypi, version:" $VERSION

GIT=`which git`

$GIT add .
$GIT comment -m "version $VERSION"
$GIT push origin master

$GIT tag -a $VERSION -m "version $VERSION"


