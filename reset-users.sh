#!/bin/bash
#
# USAGE: sudo bash reset-users.sh
# 
# Clears all the data from the student directories (in HDFS as well)
# removes the users, and creates them anew

set -e

USERS_NUMBERS=$(seq 1 99)

for U_NUM in $USERS_NUMBERS
do
  USERNAME=$(printf "group%02d" $U_NUM)

  # Clean up the previous user instance
  if id $USERNAME >/dev/null 2>&1
  then
    echo "user found $USERNAME, deleting"
    deluser --remove-home $USERNAME
  fi

  echo "Creating user $USERNAME"
  PWD=$(printf "%spwd" $USERNAME)
  useradd -g students --groups hadoop,spark -m --password $PWD --shell /bin/bash $USERNAME

  # echo "Setting up hadoop directories for user $USERNAME"
  sudo -u hadoop /opt/hadoop/bin/hdfs dfs -rm -r /user/$USERNAME || true
  sudo -u hadoop /opt/hadoop/bin/hdfs dfs -mkdir -p /user/$USERNAME
  sudo -u hadoop /opt/hadoop/bin/hdfs dfs -chown -R $USERNAME:$USERNAME /user/$USERNAME
  sudo -u hadoop /opt/hadoop/bin/hdfs dfsadmin -setSpaceQuota 10G /user/$USERNAME

done

