#!/bin/bash
HOSTIP=$(hostname -I | awk '{print $1}')
#
echo "deb http://90.147.77.142/repo/htcondor/ubuntu/previous/ trusty contrib" >> /etc/apt/sources.list
wget -qO - http://90.147.77.142/repo/htcondor/ubuntu/HTCondor-Release.gpg.key | apt-key add -
apt-get update
sleep 5
apt-get -y install condor
#
CUID=$(id condor | awk '{print $1}'| cut -d'=' -f2|cut -d'(' -f1)
CGID=$(id condor | awk '{print $2}'| cut -d'=' -f2|cut -d'(' -f1)
#
cat << EOF >/etc/condor/condor_config.local
######################################################################
##
##  condor_config.local.dedicated.resource
##
##  This is the default local configuration file for any resources
##  that are going to be configured as dedicated resources in your
##  Condor pool.  If you are going to use Condor's dedicated MPI
##  scheduling, you must configure some of your machines as dedicated
##  resources, using the settings in this file.
##
##  PLEASE READ the discussion on "Configuring Condor for Dedicated
##  Scheduling" in the "Setting up Condor for Special Environments"
##  section of the Condor Manual for more details.
##
##  You should copy this file to the appropriate location and
##  customize it for your needs.  The file is divided into three main
##  parts: settings you MUST customize, settings regarding the policy
##  of running jobs on your dedicated resources (you must select a
##  policy and uncomment the corresponding expressions), and settings
##  you should leave alone, but that must be present for dedicated
##  scheduling to work.  Settings that are defined here MUST BE
##  DEFINED, since they have no default value.
##
######################################################################

SEC_DAEMON_AUTHENTICATION = required
SEC_DAEMON_INTEGRITY = required
SEC_DAEMON_AUTHENTICATION_METHODS = password
SEC_CLIENT_AUTHENTICATION_METHODS = password,fs,gsi,kerberos
SEC_PASSWORD_FILE = /etc/condor/condor_credential
SEC_ENABLE_MATCH_PASSWORD_AUTHENTICATION = True

CONDOR_HOST = %ipv4%
CONDOR_ADMIN = root@%ipv4%

# Preserve UID of submitting user
UID_DOMAIN = *
TRUST_UID_DOMAIN = True
SOFT_UID_DOMAIN = True

CONDOR_IDS = $CUID.$CGID
QUEUE_SUPER_USERS = root, condor
HIGHPORT = 42000
LOWPORT = 41000
NETWORK_INTERFACE = $HOSTIP
NO_DNS = True
DEFAULT_DOMAIN_NAME = INFN-PD

######################################################################
######################################################################
##  Settings you MUST customize!
######################################################################
######################################################################

##  What is the name of the dedicated scheduler for this resource?
##  You MUST fill in the correct full hostname where you're running
##  the dedicated scheduler, and where users will submit their
##  dedicated jobs.  The "DedicateScheduler@" part should not be
##  changed, ONLY the hostname.
DedicatedScheduler = "DedicatedScheduler@"


######################################################################
######################################################################
##  Policy Settings (You MUST choose a policy and uncomment it) 
######################################################################
######################################################################

##  There are three basic options for the policy on dedicated
##  resources: 
##  1) Only run dedicated jobs
##  2) Always run jobs, but prefer dedicated ones
##  3) Always run dedicated jobs, but only allow non-dedicated jobs to
##     run on an opportunistic basis.   
##  You MUST uncomment the set of policy expressions you want to use
##  at your site.

##--------------------------------------------------------------------
## 1) Only run dedicated jobs
##--------------------------------------------------------------------
#START          = Scheduler =?= \$(DedicatedScheduler)
#SUSPEND        = False
#CONTINUE       = True
#PREEMPT        = False
#KILL           = False
#WANT_SUSPEND   = False
#WANT_VACATE    = False
#RANK           = Scheduler =?= \$(DedicatedScheduler)

##--------------------------------------------------------------------
## 2) Always run jobs, but prefer dedicated ones
##--------------------------------------------------------------------
START           = True
SUSPEND = False
CONTINUE        = True
PREEMPT = False
KILL            = False
WANT_SUSPEND    = False
WANT_VACATE     = False
RANK            = Scheduler =?= \$(DedicatedScheduler)

##--------------------------------------------------------------------
## 3) Always run dedicated jobs, but only allow non-dedicated jobs to
##    run on an opportunistic basis.   
##--------------------------------------------------------------------
##  Allowing both dedicated and opportunistic jobs on your resources
##  requires that you have an opportunistic policy already defined.
##  These are the only settings that need to be modified from your
##  existing policy expressions to allow dedicated jobs to always run
##  without suspending, or ever being preempted (either from activity
##  on the machine, or other jobs in the system).

#SUSPEND        = Scheduler =!= \$(DedicatedScheduler) && (\$(SUSPEND))
#PREEMPT        = Scheduler =!= \$(DedicatedScheduler) && (\$(PREEMPT))
#RANK_FACTOR    = 1000000
#RANK   = (Scheduler =?= \$(DedicatedScheduler) * \$(RANK_FACTOR)) + \$(RANK)
#START  = (Scheduler =?= \$(DedicatedScheduler)) || (\$(START))

##  Note: For everything to work, you MUST set RANK_FACTOR to be a
##  larger value than the maximum value your existing rank expression
##  could possibly evaluate to.  RANK is just a floating point value,
##  so there's no harm in having a value that's very large.


######################################################################
######################################################################
##  Settings you should leave alone, but that must be defined
######################################################################
######################################################################

##  Path to the special version of rsh that's required to spawn MPI
##  jobs under Condor.  WARNING: This is not a replacement for rsh,
##  and does NOT work for interactive use.  Do not use it directly!
MPI_CONDOR_RSH_PATH = \$(LIBEXEC)

##  Path to OpenSSH server binary
##  Condor uses this to establish a private SSH connection between execute
##  machines. It is usually in /usr/sbin, but may be in /usr/local/sbin
CONDOR_SSHD = /usr/sbin/sshd

##  Path to OpenSSH keypair generator.
##  Condor uses this to establish a private SSH connection between execute
##  machines. It is usually in /usr/bin, but may be in /usr/local/bin
CONDOR_SSH_KEYGEN = /usr/bin/ssh-keygen

##  This setting puts the DedicatedScheduler attribute, defined above,
##  into your machine's classad.  This way, the dedicated scheduler
##  (and you) can identify which machines are configured as dedicated
##  resources.
##  Note: as of 8.4.1 this setting is automatic
#STARTD_EXPRS = \$(STARTD_EXPRS), DedicatedScheduler

EOF

# Store credentials
/usr/sbin/condor_store_cred -p 12345 add -f /etc/condor/condor_credential

# Start condor
service condor start

##########################################
##
## Setup Spark
##
##########################################

## TODO: Move to external configuration file
## Configuration
SPARK_VERSION=2.2.0
SPARK_HOME=/opt/spark

echo "Adding Spark group and user"
# Create a Spark user and a Spark group
groupadd spark
useradd -G spark spark

# Download spark
echo "Installing Java"
apt-get update
apt-get -y install openjdk-9-jdk-headless

if [[ ! -d $SPARK_HOME ]]
then
    echo "Downloading Spark"
    SPARK_URL=http://it.apache.contactlab.it/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop2.7.tgz
    curl -o /tmp/spark.tgz $SPARK_URL

    echo "Unpacking Spark"
    cd /opt
    tar xzvf /tmp/spark.tgz
    mv spark-$SPARK_VERSION-bin-hadoop2.7 spark
fi

chown -R spark:spark $SPARK_HOME

cat <<EOF > $SPARK_HOME/conf/spark-env.sh
SPARK_LOCAL_IP=$HOSTIP
SPARK_MASTER_HOST=%ipv4%
EOF
chdmod +x $SPARK_HOME/conf/spark-env.sh

