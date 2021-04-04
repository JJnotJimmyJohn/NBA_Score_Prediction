echo "Macbook cron job started: $(date)" >> /Users/jianjin/Documents/NBA_Score_Prediction/mylog.log
export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
docker exec 8f5d2093f3ce bash /Documents/NBA_Score_Prediction/run_in_docker.sh
echo "Macbook cron job ended: $(date)" >> /Users/jianjin/Documents/NBA_Score_Prediction/mylog.log