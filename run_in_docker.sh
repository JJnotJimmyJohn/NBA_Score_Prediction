#!/bin/bash
# TODO: rebuild the docker image using dockerfile.
# TODO: run stuff using container, instead of keeping the container running
# TODO: use Ubuntu in new docker image
# TODO: set timezone in dockerfile
    # $ echo "Australia/Adelaide" | sudo tee /etc/timezone
    # Australia/Adelaide 
    # $ sudo dpkg-reconfigure --frontend noninteractive tzdata
cd "${0%/*}"
python /home/NBA_Score_Prediction/utils/datarefresher.py
python /home/NBA_Score_Prediction/utils/dataprocesser.py
################# below is the daily script ###################
# echo "Start Time: $(date)" >> ./crontab_log.txt

# sudo docker exec -e MONGODB_PWD=$MONGODB_PWD -e MONGODB_USERNAME=$MONGODB_USERNAME -e MONGODB_ENDPOINT=$MONGODB_ENDPOINT 92eb12673e15 bash /home/NBA_Score_Prediction/run_in_docker.sh

# echo "End Time: $(date)" >> ./crontab_log.txt