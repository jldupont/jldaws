#!/bin/sh

echo "Committing & Pushing to github: " $1
GIT=`which git`

$GIT add .
$GIT commit -m "$1"
$GIT push origin master

echo "Submitting egg to Pypi"
python setup.py sdist --formats=zip,gztar upload

