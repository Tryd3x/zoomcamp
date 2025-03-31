set -e

TAXI_TYPE=$1 # "yellow"
YEAR=$2 #2020

# Example URL to download a file: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..12}
    do
        FMONTH=$(printf "%02d" ${MONTH})
        URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

        LOCAL_PREFIX="./datasets/raw/${TAXI_TYPE}/${YEAR}" # Path to store downloaded files
        LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet" # File name of the downloaded file
        LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}" # Path + Filename

        mkdir -p ${LOCAL_PREFIX} # Create directory if not exist
        echo "downloading ${URL} to ${LOCAL_PATH}"
        wget ${URL} -O ${LOCAL_PATH}

        # You can add error handling to notify if previous command failed
        if [ $? -ne 0 ]
            then 
                echo "[Error]: Failed to download ${LOCAL_FILE}"
        fi
done 