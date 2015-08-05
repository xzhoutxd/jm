#!/bin/sh

DATESTR=`date +"%Y%m%d%H"`

if [ $# = 0 ]; then
    echo " Usage: $0 master|slave" 
    echo " e.g. : $0 m|s" 
    exit 1
else
    m_type=$1
fi
DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JMGlobal python python

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jm/global/add_channel_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JMGlobal.py $m_type > $LOGFILE

# process queue
p_num=2
obj='globalitem'
crawl_type='main'
DIR=`pwd`
cd $DIR
/bin/sh $DIR/k.sh JMWorkerM $obj $crawl_type

cd $DIR/../..
LOGDIR=`pwd`
LOGFILE=$LOGDIR/logs/jm/global/add_items_${DATESTR}.log

cd $DIR
/usr/local/bin/python $DIR/JMWorkerM.py $p_num $obj $crawl_type > $LOGFILE

