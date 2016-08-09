#!/bin/bash
CURDIR=$(dirname "$0")
ROOTDIR="$CURDIR/.."
EXPECTED_MASTER_IP="10.10.4.2"
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color
HOSTS_LIST_CACHE=""

## Dependency Checks

deps="vagrant VBoxManage"
for dep in $deps; do
    which $dep > /dev/null || (echo "Cannot find command: $dep, install it and run again" && exit 1)
done

## Prompts

success(){
    echo -e "${GREEN}Success: ${NC}" $1
}

failed(){
    echo -e "${RED}Failed: ${NC}" $1 "Result[" $2 "]" "Expected[" $3 "]"
}

## Vagrant specific checks

get_hosts(){
    if [ "x$HOSTS_LIST_CACHE" = "x" ];
    then
       HOSTS_LIST_CACHE=$(vagrant hosts list)
    fi
    echo "${HOSTS_LIST_CACHE}"
}


get_actual_host_ip(){
    HOST_NAME=$1
    vagrant ssh $HOST_NAME <<EOF | grep -A1 eth1 | perl -lne 'print $1 if /inet addr:(\S+)/'
ifconfig
EOF
}

get_slaves_count(){
    if [ "x$MESOS_SLAVES" != "x" ];
    then
        return $MESOS_SLAVES;
    else
        echo $(grep MESOS_SLAVES $ROOTDIR/Vagrantfile | tr -d '[:alpha:][:punct:][:blank:]');
    fi
}

## Validation code


validate_newest_box(){
    vagrant box outdated
    if [ "$?" = "0" ];
    then
        success "Vagrant Box is upto date"
    else
        failed "Vagrant Box needs to be updated. Please run vagrant box update"
    fi
}

validate_master_ip(){
    master_ip=$(get_hosts | grep master | cut -f1)
    echo $master_ip | grep $EXPECTED_MASTER_IP && success "Valid Master IP $EXPECTED_MASTER_IP" || failed "Invalid Master IP" $master_ip $EXPECTED_MASTER_IP
}

validate_slave_counts(){
    expected_slaves_count=$(get_slaves_count)
    total_slaves=$(get_hosts | grep slave | wc -l)
    echo $total_slaves | grep $expected_slaves_count && success "Valid number of slaves $expected_slaves_count" || failed "Slave counts don't match" $total_slaves $expected_slaves_count
}


validate_master_ip_matches_with_vagrantfile(){
    vagrant_master_ip=$(get_hosts | grep master | cut -f1)
    actual_master_ip=$(get_actual_host_ip master)

    echo $actual_master_ip | grep $EXPECTED_MASTER_IP && success "Master IP in the machine matches with $EXPECTED_MASTER_IP" || failed "Invalid Master IP for the machine than what vagrant expects" $actual_master_ip $EXPECTED_MASTER_IP
}

validate_zookeeper_running(){
    vagrant ssh master <<EOF | grep imok
echo ruok | nc localhost 2181
EOF
    if [ "$?" = "0" ];
    then
        success "Zookeeper is running on Master"
    else
        failed "Zookeeper is not running on Master"
    fi
}

validate_mesos_master_running(){
    vagrant ssh master <<EOF | grep running
sudo service mesos-master status
EOF
    if [ "$?" = "0" ];
    then
        success "mesos-master service is running"
        MASTER_URL="http://$EXPECTED_MASTER_IP:5050"
        vagrant ssh master <<EOF | grep 200
curl -IsS $MASTER_URL
EOF
        if [ "$?" = "0" ];
        then
            success "mesos-master is listening on $MASTER_URL"
        else
            failed "mesos-master is running but not listening on $MASTER_URL"
        fi
    else
        failed "mesos-master service is not running"
    fi
}

validate_slaves_reachable_from_master(){
    slave_ips=$(get_hosts | grep slave | cut -d' ' -f1)
    for ip in $slave_ips ; do
        echo "Checking $ip"
        #name=$(get_hosts | grep "$ip" | cut -d' ' -f2)
        SLAVE_URL="http://$ip:5051/state.json"
        vagrant ssh master <<EOF | grep 200
curl -IsS $SLAVE_URL
EOF
        if [ "$?" = "0" ];
        then
            success "mesos-slave on $ip is reachable from Master"
        else
            failed "mesos-slave on $ip is not reachable from Master"
        fi
    done
}

validate_master_reachable_from_slaves(){
    slave_names=$(get_hosts | grep slave | cut -d' ' -f3)
    for name in $slave_names; do
        echo "Checking from $name"
        MASTER_URL="http://$EXPECTED_MASTER_IP:5050"
        vagrant ssh $name <<EOF | grep 200
curl -IsS $MASTER_URL
EOF
        if [ "$?" = "0" ];
        then
            success "mesos-master on $MASTER_URL is rechable from $name"
        else
            failed "mesos-master on $MASTER_URL is not reachable from $name"
        fi
    done
}


validate_newest_box
validate_master_ip
validate_zookeeper_running
validate_slave_counts
validate_master_ip_matches_with_vagrantfile
validate_mesos_master_running
validate_slaves_reachable_from_master
validate_master_reachable_from_slaves
