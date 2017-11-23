export FLAVOR=cloudveneto.medium
export MAX_VMS=3
export MIN_VMS=2
export JOBS_PER_VM=2
export IDLE_TIME=2400
export KEY_NAME=cloudveneto-machines

## EC2-id of the image to be used for minions. Uncomment your choice
##
##  Fedora 23:            
#export IMAGE_ID=ami-00000027
## ubuntu-16.04-spark-2.2.0-hadoop-2.7
export IMAGE_ID=ami-0000010a
##  ubuntu-16.04-oracle-java-9
# export IMAGE_ID=ami-00000101
##  ubuntu-16.04.1-LTS:   
#export IMAGE_ID=ami-000000c6
##  ubuntu-14.04.3-LTS:   
#export IMAGE_ID=ami-0000001b
##  uCernVM3:             
#export IMAGE_ID=ami-0000005a
##  CentOS 7:             
#export IMAGE_ID=ami-00000013
##  CentOS 6:             
#export IMAGE_ID=ami-00000010
##  Custom. You need to know the EC2-id of the image
#export IMAGE_ID=

# Spark configuration
export SPARK_VERSION=2.2.0
export SPARK_HOME=/opt/spark

