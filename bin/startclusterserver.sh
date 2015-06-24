#!/bin/sh
export COMMUNITY_HOME=/home/xiafan/git/comdetection
export PYTHONPATH=$PYTHONPATH:$COMMUNITY_HOME/src:$COMMUNITY_HOME/src/comdetection:$COMMUNITY_HOME/src/dao:$COMMUNITY_HOME/src/weibocrawler

python $COMMUNITY_HOME/src/comdetection/clusterworker.py