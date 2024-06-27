#!/bin/bash
ref_date=$(date +'%Y-%m-%d')

start_date="20240625"
end_date="20240625"


output_base_dir="/home/fox/Desktop/july_onwards_daily_reports/daily_data/${ref_date}/"

declare -A urls_patterns
urls_patterns["isone_node_dam_lmp"]="https://www.iso-ne.com/static-transform/csv/histRpts/da-lmp/WW_DALMP_ISO_{date}.csv"
urls_patterns["isone_node_twha_lmp"]="https://www.iso-ne.com/static-transform/csv/histRpts/rt-lmp/lmp_rt_final_{date}.csv"
url_pattern_rtm_lmp="https://www.iso-ne.com/static-transform/csv/histRpts/5min-rt-final/lmp_5min_{date}_{part}.csv"

download_file() {
    local url=$1
    local output_file=$2
    local retries=3
    local count=0

    while [ $count -lt $retries ]; do
        curl -L -f -s -S -o "$output_file" "$url"
        
        http_status=$(curl -o /dev/null -s -w "%{http_code}\n" -L "$url")
        echo "HTTP status code for $url: $http_status"

        if [ ! -s "$output_file" ] || [ "$http_status" -ne 200 ]; then
            echo "Downloaded file $output_file is empty or download failed. Attempt $((count+1))/$retries."
            rm -f "$output_file"
            count=$((count+1))
            if [ $count -lt $retries ]; then
                echo "Retrying after 10 seconds..."
                sleep 10
            fi
        else
            echo "Downloaded file $output_file successfully."
            return 0
        fi
    done

    echo "Failed to download $url after $retries attempts."
    return 1
}

download_single_date_files() {
    local current_date="$start_date"
    while [ "$current_date" != $(date -I -d "$end_date + 1 day") ]; do
        for key in "${!urls_patterns[@]}"; do
            url="${urls_patterns[$key]}"
            url=${url//"{date}"/$current_date}
            output_dir="${output_base_dir}${key}/"
            mkdir -p "$output_dir"
            original_filename=$(basename "$url")
            output_file="${output_dir}${original_filename}"
            echo "Downloading $url to $output_file"
            download_file "$url" "$output_file"
        done
        current_date=$(date -I -d "$current_date + 1 day")
    done
}

download_rtm_lmp_files() {
    local date=$1
    local parts=("00-04" "04-08" "08-12" "12-16" "16-20" "20-24")
    
    for part in "${parts[@]}"; do
        url=${url_pattern_rtm_lmp//"{date}"/$date}
        url=${url//"{part}"/$part}
        
        output_dir="${output_base_dir}isone_node_rtm_lmp/"
        mkdir -p "$output_dir"
        
        original_filename=$(basename "$url")
        
        echo "Downloading $url to $output_dir$original_filename"
        download_file "$url" "$output_dir$original_filename"
    done
}

download_single_date_files
download_rtm_lmp_files "$start_date"

echo "Download script completed."
