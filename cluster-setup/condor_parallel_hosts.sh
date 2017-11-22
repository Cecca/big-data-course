#!/bin/bash

##**************************************************************
## condor_parallel_hosts.sh 
##       Created by I.Sz. <szebi@xxxxxxxxxx> BME-IIT 2012.07.17
## Functions for collecting host and job information about the running parallel job.
## Function CONDOR_PARALLEL_HOSTS creates a hostfile including contact info for remote hosts
## Usage: Source the script and use the CONDOR_GET_PARALLEL_HOSTS_INFO function
##************************************************************** 

# Defaults for error testing
: ${_CONDOR_PROCNO:=0}
: ${_CONDOR_NPROCS:=1}
: ${_CONDOR_MACHINE_AD:="None"}
: ${_CONDOR_JOB_AD:="None"}

##************************************************************** 
## Usage: CONDOR_GET_PARALLEL_HOSTS_INFO [hostfile]
## If hostfile omitted 'parallel_hosts' is used.
## Return:
##   The function returns with error status on main process (_CONDOR_PROCNO==0).
##   The function never returns on on the other nodes (sleeping).
## The created file structure: 
##   HostName1'-CONDOR-'CLusterID.ProcId.SubProcId 'slots='Allocated_CPUs 'max_slots='Allocated_CPUs
##   HostName2'-CONDOR-'CLusterID.ProcId.SubProcId 'slots='Allocated_CPUs 'max_slots='Allocated_CPUs
##   HostName3'-CONDOR-'CLusterID.ProcId.SubProcId 'slots='Allocated_CPUs 'max_slots='Allocated_CPUs
##   ...
##************************************************************** 
function CONDOR_GET_PARALLEL_HOSTS_INFO() {
    # getting parameters if _CONDOR_PARALLEL_HOSTS_FILE not set
    : ${_CONDOR_PARALLEL_HOSTS_FILE:=$1}
    # setting defaults
    : ${_CONDOR_PARALLEL_HOSTS_FILE:=parallel_hosts}
    local hostname=`hostname -f`
    if [ $_CONDOR_PROCNO -eq 0 ]; then
    # collecting info on the main proc
        clusterid=`CONDOR_GET_JOB_ATTR ClusterId`
        local ret=$?
        if [ $ret -ne 0 ]; then 
            echo Error: get_job_attr ClusterId
            return 1
        fi
        local line=""
        condor_q -l $clusterid | \
        awk '/^ProcId.=/ { ProcId=$3 } \
             /^ClusterId.=/ { ClusterId=$3 } \
             /^RequestCpus.=/ { RequestCpus=$3 } \
             /^RemoteHosts.=/ { RemoteHosts=$3 } \
             /^$/ { if (ClusterId != 0) print ClusterId" "ProcId" "RequestCpus" "RemoteHosts  }' | \
        while read line; do
            CONDOR_PRINT_HOSTS $line
        done | sort -d > ${_CONDOR_PARALLEL_HOSTS_FILE}
    else 
    # endless loop on the workers
        while true ; do
            sleep 30
        done
    fi
    return 0
}

## Helper fn for getting specific machine attributes from $_CONDOR_MACHINE_AD
function CONDOR_GET_MACHINE_ATTR() {
    local attr="$1"
    awk '/^'"$attr"'[[:space:]]+=[[:space:]]+/ \
        { ret=sub(/^'"$attr"'[[:space:]]+=[[:space:]]+/,""); print; } \
        END { exit 1-ret; }' $_CONDOR_MACHINE_AD
    return $?
} 

## Helper fn for getting specific job attributes from $_CONDOR_JOB_AD
function CONDOR_GET_JOB_ATTR() {
    local attr="$1"
    awk '/^'"$attr"'[[:space:]]+=[[:space:]]+/ \
        { ret=sub(/^'"$attr"'[[:space:]]+=[[:space:]]+/,""); print; } \
        END { exit 1-ret; }' $_CONDOR_JOB_AD
    return $?
} 

## Helper fn for printing the host info
function CONDOR_PRINT_HOSTS() {
    local clusterid=$1
    local procid=$2
    local reqcpu=$3
    local rhosts=$4
    tr ',"' '\n' <<< $rhosts | grep -v $hostname | \
    awk '{ sub(/slot.*@/,""); if ($1 != "") { slots[$1]+='$reqcpu'; subproc[$1]=id++; } } \
        END { for (i in slots) print i"-CONDOR-"'$clusterid'".1."subproc[i]" slots="slots[i]" max_slots="slots[i]; }' 
}
