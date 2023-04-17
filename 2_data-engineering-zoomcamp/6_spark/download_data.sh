#!/bin/bash

set -e
#set -x

TAXI_TYPE=$1
YEAR=$2

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for month in {1..12}; do

    month=$(printf '%02d' ${month})
    #month=`printf '%02d' ${month}`
    FOLDER="data/raw/${TAXI_TYPE}/${YEAR}/${month}"
    FILE="${TAXI_TYPE}_tripdata_${YEAR}_${month}.csv.gz"
    FINAL_PATH="${FOLDER}/${FILE}"

    URL=${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${month}.csv.gz
    echo "downloading ${URL} to ${FINAL_PATH}"
    mkdir -p ${FOLDER}

    wget -O ${FINAL_PATH} ${URL} || (echo "URL ${URL} NOT FOUND" && rm -rf ${FOLDER})

done