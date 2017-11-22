#!/bin/bash

SPARK_HOME=$SPARK_HOME
SLAVES_FILE=$SPARK_HOME/conf/slaves
MASTER_FILE=$SPARK_HOME/conf/master
SPARK_SBIN=$SPARK_HOME/sbin/

IP_ADDR=$(ip addr | egrep -o '10.67.[0-9]+.[0-9]+' | head -n1)

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
  MASTER=$(print_master $CONDOR_JOB_DESCRIPTION_FILE)
  cat EOF<< > $SPARK_HOME/conf/spark-env.sh
    SPARK_LOCAL_IP=$IP_ADDR
    SPARK_MASTER_HOST=$MASTER
  EOF
}

function is_spark_running () {
  MASTER=$(print_master $1)
  curl -s http://$MASTER:6066/ > /dev/null
}

function restart_spark () {
  if is_spark_running $1; then
    $SPARK_SBIN/stop-all.sh
  fi
  $SPARK_SBIN/start-all.sh
}

if [ -z ${_CONDOR_JOB_AD+x} ]
then
echo "You are not calling me from a Condor job"
exit 1
fi

configure_cluster $_CONDOR_JOB_AD
restart_spark $_CONDOR_JOB_AD