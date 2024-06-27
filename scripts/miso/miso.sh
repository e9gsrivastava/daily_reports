#!/bin/bash

ref_date=$(date +'%Y-%m-%d')

output_base_dir="/home/fox/Desktop/july_onwards_daily_reports/daily_data/${ref_date}/"

mkdir -p "$output_base_dir"
declare -A report_dates
report_dates=(
    ["miso_busbar_dam_lmp"]="2024-06-18 2024-06-20"
    ["miso_node_twha_lmp"]="2024-06-21 2024-06-23"
    ["miso_node_dam_lmp"]="2024-06-24 2024-06-26"
    ["miso_busbar_twha_lmp"]="2024-06-18 2024-06-22"
    ["miso_as_dam_reg_spin_nonspin"]="2024-06-19 2024-06-25"
    ["miso_as_twha_reg_spin_nonspin"]="2024-06-20 2024-06-26"
    ["miso_node_rtm_lmp"]="2024-06-21 2024-06-25"
    ["miso_as_rtm_reg_spin_nonspin"]="2024-06-22 2024-06-26"
)

download_file() {
    local url=$1
    local folder_name=$2
    local date=$3

    output_dir="${output_base_dir}/${folder_name}"
    mkdir -p "$output_dir"
    output_file="${output_dir}/$(basename "$url")"

    curl -o "$output_file" "$url"

    if [[ $? -ne 0 ]]; then
        echo "Failed to download $url"
    else
        echo "Downloaded $url to $output_file"
    fi
}

for folder_name in "${!report_dates[@]}"; do
    IFS=' ' read -r start_date end_date <<< "${report_dates[$folder_name]}"
    current_date=$(date -d "$start_date" +%Y%m%d)
    end_date=$(date -d "$end_date" +%Y%m%d)

    while [[ $current_date -le $end_date ]]; do
        case $folder_name in
            "miso_busbar_dam_lmp")
                download_file "https://docs.misoenergy.org/marketreports/DA_Load_EPNodes_${current_date}.zip" "$folder_name" "$current_date"
                ;;
            "miso_node_twha_lmp")
                download_file "https://docs.misoenergy.org/marketreports/${current_date}_rt_lmp_final.csv" "$folder_name" "$current_date"
                ;;
            "miso_node_dam_lmp")
                download_file "https://docs.misoenergy.org/marketreports/${current_date}_da_expost_lmp.csv" "$folder_name" "$current_date"
                ;;
            "miso_busbar_twha_lmp")
                download_file "https://docs.misoenergy.org/marketreports/RT_Load_EPNodes_${current_date}.zip" "$folder_name" "$current_date"
                ;;
            "miso_as_dam_reg_spin_nonspin")
                download_file "https://docs.misoenergy.org/marketreports/${current_date}_asm_expost_damcp.csv" "$folder_name" "$current_date"
                ;;
            "miso_as_twha_reg_spin_nonspin")
                download_file "https://docs.misoenergy.org/marketreports/${current_date}_asm_rtmcp_final.csv" "$folder_name" "$current_date"
                ;;
            "miso_node_rtm_lmp")
                download_file "https://docs.misoenergy.org/marketreports/${current_date}_5min_exante_lmp.xlsx" "$folder_name" "$current_date"
                ;;
            "miso_as_rtm_reg_spin_nonspin")
                download_file "https://docs.misoenergy.org/marketreports/${current_date}_5min_exante_mcp.xlsx" "$folder_name" "$current_date"
                ;;
        esac

        current_date=$(date -d "$current_date +1 day" +%Y%m%d)
    done
done

echo "Download and file organization script completed."
