#!/bin/bash

SPARK_HOME=/opt/spark/
SLAVES_FILE=/opt/spark/conf/slaves
MASTER_FILE=/opt/spark/conf/master
SPARK_SBIN=/opt/spark/sbin/

function print_slaves () {
  JOB_DESCRIPTION_FILE=$1
  grep "^RemoteHosts" $JOB_DESCRIPTION_FILE |\
      awk '{print $3}' |\
      sed 's/"//g' |\
      tr ',' '\n' |\
      grep -o '[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]' |\
      sed 's/-/./g' |\
      sort |\
      uniq
}

function print_master () {
  JOB_DESCRIPTION_FILE=$1
  grep "^RemoteHost " $JOB_DESCRIPTION_FILE |\
      awk '{print $3}' |\
      sed 's/"//g' |\
      grep -o '[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]' |\
      sed 's/-/./g' > /dev/null
  echo "pcceccarel"
}

function configure_cluster () {
  CONDOR_JOB_DESCRIPTION_FILE=$1
  print_slaves $CONDOR_JOB_DESCRIPTION_FILE > $SLAVES_FILE
  print_master $CONDOR_JOB_DESCRIPTION_FILE > $MASTER_FILE
}

function is_spark_running () {
  MASTER=$(print_master $1)
  echo "Master: $MASTER"
  curl -s http://$MASTER:6066/ > /dev/null
}

function restart_spark () {
  if is_spark_running $1; then
    $SPARK_SBIN/stop-all.sh
  fi
  $SPARK_SBIN/start-all.sh
}



# Local Variables:
# sh-basic-offset: 2
# End:
