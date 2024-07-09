#!/bin/bash

# The first command line argument is the port to monitor
PORT=8443
HERITRIX_JOB="astro_crawler"
HERITRIX_JOB_PATH="/mnt/tianwen-course/tianwen/home/wenyh/crawler/heritrix_astro_crawler/jobs"
BASHRC_PATH="/mnt/tianwen-course/tianwen/home/wenyh/.zshrc"

echo $$ >> .monitor_pid

# Check if the port number is provided
if [ -z "$PORT" ]; then
    echo "Usage: $0 <port>"
    exit 1
fi

# Continuously monitor the specified port
while true; do
    # Use the ss command to check if the port is in use without resolving service names
    if ! netstat -tulnp | grep ":$PORT" > /dev/null; then
        echo "Port $PORT is not in use. Starting the crawl process..."

        # Execute commands to start the crawl process
        # Note: Adjust the following commands according to your actual environment and requirements
        source $BASHRC_PATH
        heritrix -a admin:admin -j $HERITRIX_JOB_PATH -p $PORT
        heri build $HERITRIX_JOB
        heri start $HERITRIX_JOB
        heri continue $HERITRIX_JOB
    fi

    # Wait for a specified interval before checking again, set here to 30 seconds
    sleep 1800
done

