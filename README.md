
### Scripts and Usage Guide

#### MyISO
- **Script Name:** `miso.sh`
- **Usage:** Provide start date and end date and run the script. Specify the path where you want to download and maintain directory structure.
- **Directory Structure:**
  ```
  <output_path>/
  ├── ref_date/
  │   ├── <file1>.csv
  │   ├── <file2>.csv
  │   └── ...
  ```

#### CAISO
- **Scripts:**
  - `caiso.py` 
- **Usage:**
  1. Run `caiso.py`, providing the start date and end date, along with the download path.
- **Directory Structure:** Same as MyISO.

#### NYISO
- **Script Name:** `nyiso.sh`
- **Usage:** Provide start date and end date and run the script. Specify the path where you want to download and maintain directory structure.
- **Directory Structure:** Same as MyISO.

#### PJM
- **Script Name:** `scraper.py`
- **Usage:** Provide start date and end date and run the script. You may have to add `Ocp-Apim-Subscription-Key`, which may remain unchanged for weeks. Specify the path where you want to download and maintain directory structure.
- **Directory Structure:** Same as MyISO.

#### ISONE
- **Scripts:**
  - `node.sh` (For Node DAM, Node RTM and Node TWHA)
- **Usage:**
  1. Run `node.sh` for Node DAM and Node TWHA and Node RTM, providing start date, end date, and download path.
  2. Ancillary files are download manually.
- **Directory Structure:** Same as MyISO.

#### ERCOT
- These are downloaded manually.
- **Directory Structure:** Same as MyISO.

### Notes:
- The directory path for all the scripts must be the same to aggregate all files for a day into a single folder.
- Specify start and end dates separately for all reports.
- Ensure to install required packages from `requirements.txt`.
- The scripts automatically use the current date as `ref_date` and create the directory structure `<ref_date/report_id/abc.csv>`.
- Create an excel file for gaps till date.

### Checker Output Description
- **Scripts:**
  - `checker.py` To validate the files are correctly downlaoded.
- **Usage:**
  1. Run `checker.py` provide the path for a ref date.

The output file will have the following columns:

| Column Name           | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| report_id             | Identifier for the type of report (e.g., `ercot_node_dam_lmp`, `nyiso_node_dam_lmp`) |
| folder_size_kb        | Size of the folder in kilobytes where the files are stored                   |
| file_count            | Number of files present in the folder                                        |
| Expected_file_count   | Expected number of files based on the date range provided                    |
| expected_folder_size  | Expected size of the folder in kilobytes based on the expected file count    |
| headers_match_or_not  | Indicates if headers of files inside each report_id match the expected headers |

### Example:
If the `ref_date` contains files for two days, then `file_count` should be twice `Expected_file_count`, and `folder_size_kb` should be twice `expected_folder_size`. `headers_match_or_not` ensures each file has correct headers corresponding to its `report_id`.



