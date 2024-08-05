#!/bin/bash

bucket_name="your_aws_bucket_name"
log_prefix="log_prefix"
local_folder="/tmp/unzipped_logs"
last_date_file="/tmp/last_date.txt"
log_file="/var/log/s3_logs.log"

log_message() {
    local message=$1
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $message" >> "$log_file"
    echo "$message"
}

ensure_permissions() {
    local path=$1
    local mode=$2

    if [[ ! -e "$path" ]]; then
        echo "Creating $path"
        if [[ "$mode" == "directory" ]]; then
            mkdir -p "$path"
            chmod 755 "$path"
        else
            touch "$path"
            chmod 666 "$path"
        fi
    elif [[ ! -w "$path" ]]; then
        echo "Adjusting permissions for $path"
        chmod 666 "$path"
    fi
}

log_dir=$(dirname "$log_file")
ensure_permissions "$log_dir" "directory"

if [[ -w "$log_file" || ! -e "$log_file" ]]; then
    > "$log_file"
    log_message "Log file cleared."
else
    echo "Error: No write permission for $log_file"
    exit 1
fi

ensure_permissions "$last_date_file" "file"

process_files() {
    log_message "Processing files from $log_folder."

    mkdir -p "$local_folder"

    log_files=$(aws s3 ls "s3://$bucket_name/$log_prefix/$current_date/")

    max_jobs=10
    job_count=0
    lockfile="/tmp/script.lock"
    exec 200>$lockfile

    while read -r line; do
        filename=$(echo "$line" | awk '{print $4}')
        if [[ ! -z "$filename" ]]; then
            local_gz_file="$local_folder/$filename"

            flock -n 200 || exit 1

            {
                aws s3 cp "s3://$bucket_name/$log_prefix/$current_date/$filename" - | gzip -d > "$local_folder/${filename%.gz}"
                flock -u 200
            } &

            job_count=$((job_count + 1))

            if [[ $job_count -ge $max_jobs ]]; then
                wait -n
                job_count=$((job_count - 1))
            fi
        fi
    done <<< "$log_files"

    wait

    rm -f "$lockfile"

    chmod 777 -R "$local_folder"

    log_message "All downloads and unzipping completed."
}

current_date=$(date +"%Y%m%d")
echo "$current_date" > "$last_date_file"

log_message "Script started."

loop_counter=1
while true; do
    log_message "Loop $loop_counter Start"

    log_folder="s3://$bucket_name/$log_prefix/$current_date"

    if [[ -f "$last_date_file" ]]; then
        last_date=$(cat "$last_date_file")
    else
        last_date=""
    fi

    if [[ "$current_date" != "$last_date" ]]; then
        log_message "Date has changed. Clearing $local_folder..."
        rm -rf "$local_folder"/*
        echo "$current_date" > "$last_date_file"
    fi

    process_files

    log_message "Loop $loop_counter End"
    loop_counter=$((loop_counter + 1))

    current_date=$(date +"%Y%m%d")
    log_folder="s3://$bucket_name/$log_prefix/$current_date"

    sleep 2
done
