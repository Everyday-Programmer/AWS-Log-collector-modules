#!/bin/bash

bucket_name="your_aws_bucket_name"
local_folder="/tmp/elb_unzipped_logs"
log_file="/tmp/s3_elb_logs.log"
last_date_file="/tmp/elb_last_date.txt"
prefixes=(
    "prefix-1"
    "prefix-2"
    "prefix-3"
)

log_message() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") $1" >> "$log_file"
    echo "$1"
}

construct_path() {
    local prefix=$1
    local year=$(date +%Y)
    local month=$(date +%m)
    local day=$(date +%d)

    echo "$prefix$year/$month/$day/"
}

process_files() {
    local prefix=$1

    local s3_path=$(construct_path "$prefix")
    log_message "Processing files from prefix: $s3_path."

    mkdir -p "$local_folder"

    file_list=$(aws s3 ls "s3://$bucket_name/$s3_path" --recursive | awk '{print $4}')
    if [[ $? -ne 0 ]]; then
        log_message "Failed to list files from S3 bucket: s3://$bucket_name/$s3_path."
        return 1
    fi

    local max_jobs=10
    local job_count=0
    local lockfile="/tmp/alb_script.lock"
    exec 200>$lockfile

    while read -r filename; do
        if [[ -z "$filename" ]]; then
            continue
        fi

        local_gz_file="$local_folder/$(basename "$filename")"
        local_unzipped_file="$local_folder/$(basename "${filename%.gz}")"

        if [[ -f "$local_unzipped_file" ]]; then
            log_message "File s3://$bucket_name/$filename already exists. Skipping download."
            continue
        fi

        flock -n 200 || exit 1

        {
            aws s3 cp "s3://$bucket_name/$filename" - 2>> "$log_file" | gzip -d > "$local_unzipped_file"

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

    chmod 777 -R "$local_folder"

    #log_message "All downloads and unzipping completed for s3://$bucket_name/$s3_path."
}

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
            echo "$current_date" > "$last_date_file"
        fi
    else
        echo "$current_date" > "$last_date_file"
    fi

    for prefix in "${prefixes[@]}"; do
        process_files "$prefix"
    done

    log_message "Loop $loop_counter End"
    loop_counter=$((loop_counter + 1))

    sleep 10
done