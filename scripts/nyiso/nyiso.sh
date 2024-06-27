#!/bin/bash

ref_date=$(date +'%Y-%m-%d')

declare -A url_patterns
declare -A report_dates

url_patterns["nyiso_as_dam_reg_spin_nonspin"]="http://mis.nyiso.com/public/csv/damasp/{date}damasp.csv"
url_patterns["nyiso_hub_dam_lmp"]="http://mis.nyiso.com/public/csv/damlbmp/{date}damlbmp_zone.csv"
url_patterns["nyiso_hub_rtm_lmp"]="http://mis.nyiso.com/public/csv/realtime/{date}realtime_zone.csv"
url_patterns["nyiso_hub_twha_lmp"]="http://mis.nyiso.com/public/csv/rtlbmp/{date}rtlbmp_zone.csv"
url_patterns["nyiso_node_dam_lmp"]="http://mis.nyiso.com/public/csv/damlbmp/{date}damlbmp_gen.csv"
url_patterns["nyiso_node_rtm_lmp"]="http://mis.nyiso.com/public/csv/realtime/{date}realtime_gen.csv"
url_patterns["nyiso_node_twha_lmp"]="http://mis.nyiso.com/public/csv/rtlbmp/{date}rtlbmp_gen.csv"
url_patterns["nyiso_as_rtm_reg_spin_nonspin"]="http://mis.nyiso.com/public/csv/rtasp/{date}rtasp.csv"

report_dates=(
    ["nyiso_as_dam_reg_spin_nonspin"]="2024-06-19 2024-06-25"
    ["nyiso_hub_dam_lmp"]="2024-06-18 2024-06-20"
    ["nyiso_hub_rtm_lmp"]="2024-06-21 2024-06-23"
    ["nyiso_hub_twha_lmp"]="2024-06-24 2024-06-26"
    ["nyiso_node_dam_lmp"]="2024-06-18 2024-06-22"
    ["nyiso_node_rtm_lmp"]="2024-06-19 2024-06-25"
    ["nyiso_node_twha_lmp"]="2024-06-20 2024-06-26"
    ["nyiso_as_rtm_reg_spin_nonspin"]="2024-06-21 2024-06-25"
)

output_base_dir="/home/fox/Desktop/july_onwards_daily_reports/daily_data/${ref_date}/"

for code in "${!url_patterns[@]}"; do
    dates=${report_dates[$code]}
    start_date=$(echo $dates | cut -d' ' -f1)
    end_date=$(echo $dates | cut -d' ' -f2)

    start_date_sec=$(date -d "$start_date" +%s)
    end_date_sec=$(date -d "$end_date" +%s)

    current_date_sec=$start_date_sec
    while [ $current_date_sec -le $end_date_sec ]; do
        current_date=$(date -d "@$current_date_sec" +%Y%m%d)

        output_dir="${output_base_dir}${code}"
        mkdir -p "$output_dir"

        url="${url_patterns[$code]}"
        url=${url//"{date}"/$current_date}
        file_name=$(basename "$url")

        retries=5
        success=false
        for ((i=1; i<=retries; i++)); do
            curl -L -o "$output_dir/$file_name" "$url"
            if [ $? -eq 0 ]; then
                success=true
                break
            else
                echo "Attempt $i failed, retrying..."
                sleep 1  
            fi
        done

        if [ "$success" = false ]; then
            echo "Failed to download $url after $retries attempts."
        fi

        current_date_sec=$((current_date_sec + 86400))
    done
done

echo "Download complete."
``
