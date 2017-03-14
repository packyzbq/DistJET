#!/bin/bash

args=

while [ -n "$1" ]; do
    args="$args $1"
    shift
done

source /afs/ihep.ac.cn/soft/juno/JUNO-ALL-SLC6/Release/J16v2r1/setup.sh
python $JUNOTESTROOT/python/JunoTest/junotest UnitTest $args