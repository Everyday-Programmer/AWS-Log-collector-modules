#!/bin/bash

bucket_name="your_aws_bucket_name"
local_folder="/tmp/ebz_cloudtrail_logs"
log_file="/tmp/s3_cloudtrail.log"
last_date_file="/tmp/cloudtrail_last_date.txt"

prefixes=(
    "prefix-1"
    "prefix-2"
    "prefix-3"
)

log_message() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") $1" >> "$log_file"
    echo "$1"
}


process_files() {
    local prefix=$1

    mkdir -p "$local_folder"

    current_date=$(date +%Y-%m-%d)
    year=$(date +%Y)
    month=$(date +%m)
    day=$(date +%d)

    s3_path="${bucket_name}/${prefix}/$year/$month/$day"
    log_message "Processing files from prefix: $s3_path."

    file_list=$(aws s3 ls s3://$s3_path/ )

    if [[ $? -ne 0 ]]; then
        log_message "Failed to list files from S3 bucket: $s3_path."
        return 1
    fi

    local max_jobs=10
    local job_count=0
    local lockfile="/tmp/ct_script.lock"
    exec 200>$lockfile

    while read -r line; do
        filename=$(echo "$line" | awk '{print $4}')
        local_gz_file="$local_folder/$(basename "$filename")"
        local_unzipped_file="$local_folder/$(basename "${filename%.gz}")"

        if [[ -f "$local_unzipped_file" ]]; then
            log_message "File s3://$s3_path/$filename already exists. Skipping download."
            continue
        fi

        flock -n 200 || exit 1

        {
            aws s3 cp "s3://$s3_path/$filename" - 2>> "$log_file" | gzip -d > "$local_unzipped_file"

            if [[ $? -ne 0 ]]; then
                log_message "Failed to download or unzip $filename"
            #else
                #log_message "Successfully downloaded and unzipped $filename"
            fi

            flock -u 200
        } &

        job_count=$((job_count + 1))

        if [[ $job_count -ge $max_jobs ]]; then
            wait -n
            job_count=$((job_count - 1))
        fi
    done <<< "$file_list"

    wait

    rm -f "$lockfile"

    #chown wazuh:wazuh -R "$local_folder"
    chmod 777 -R "$local_folder"

    #log_message "All downloads and unzipping completed for s3://$s3_path."
}


#process_files() {
#    local prefix=$1
#
#    mkdir -p "$local_folder"
#
#    current_date=$(date +%Y-%m-%d)
#    year=$(date +%Y)
#    month=$(date +%m)
#    day=$(date +%d)
#
#    s3_path="${bucket_name}/AWSLogs/341444356059/${prefix}/ap-south-1/$year/$month/$day"
#        log_message "Processing files from prefix: $s3_path."
#
#    file_list=$(aws s3 ls s3://$s3_path/ )
#
#    if [[ $? -ne 0 ]]; then
#        log_message "Failed to list files from S3 bucket: $s3_path."
#        return 1
#    fi
#
#    while read -r line; do
#        filename=$(echo "$line" | awk '{print $4}')
#        local_gz_file="$local_folder/$(basename $filename)"
#        local_unzipped_file="$local_folder/$(basename ${filename%.gz})"
#
#        if [[ -f "$local_unzipped_file" ]]; then
#            log_message "File s3://$s3_path/$filename already exists. Skipping download."
#            continue
#        fi
#
#        aws s3 cp "s3://$s3_path/$filename" - 2>> "$log_file" | gzip -d > "$local_unzipped_file" &
#
#        if [[ $? -ne 0 ]]; then
#            log_message "Failed to download or unzip $filename"
#        else
#            log_message "Successfully downloaded and unzipped $filename"
#        fi
#    done <<< "$file_list"
#
#    wait
#
#    #chown wazuh:wazuh -R "$local_folder"
#    chmod 777 -R "$local_folder"
#
#    log_message "All downloads and unzipping completed for s3://$s3_path."
#}

current_date=$(date +"%Y%m%d")
echo "$current_date" > "$last_date_file"

log_message "Script started."

loop_counter=1
while true; do
    log_message "Loop $loop_counter Start"

    current_date=$(date +%Y%m%d)
    if [[ -f "$last_date_file" ]]; then
        last_date=$(cat "$last_date_file")
        if [[ "$last_date" != "$current_date" ]]; then
            log_message "Date has changed from $last_date to $current_date. Deleting old files."
            rm -rf "$local_folder"/*
        fi
    fi

    echo "$current_date" > "$last_date_file"

    for prefix in "${prefixes[@]}"; do
        process_files "$prefix"
    done

    log_message "Loop $loop_counter End"
    loop_counter=$((loop_counter + 1))

    sleep 10
done
