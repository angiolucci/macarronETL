#!/bin/bash

CLASSPATH=../../lib/*
CLASSPATH="$CLASSPATH":.

cd build/classes
java -classpath $CLASSPATH Main $*

