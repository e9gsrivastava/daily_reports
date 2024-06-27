import glob
import logging
import multiprocessing
import os
import re
import time
from datetime import datetime, timedelta
from datetime import timezone as timez

import pytz
import requests


class CaisoScraper(object):
    """
    Class for downloading CAISO prices
    """

    def __init__(self, data_dir):
        self.datadir = data_dir
        self.baseurl = "http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname="
        self.PACIFIC_TIMEZONE = "US/Pacific"
        self.as_path = os.path.join(self.datadir, "ancillary_prices")
        self.da_path = os.path.join(self.datadir, "dam_prices", "all_nodes")
        self.rt_path = os.path.join(self.datadir, "rtm_prices", "all_nodes")

        self.REPORTS = {
            "PRC_INTVL_AS": {
                "url": "PRC_INTVL_AS&version=1&startdatetime={start_date}&enddatetime={end_date}&market_run_id=RTM&anc_type=ALL&anc_region=ALL",
                "path": os.path.join(
                    self.datadir, "caiso_as_fmm_regup_regdown_spin_nonspin"
                ),
                "frequency": "hourly",
            },
            "PRC_RTPD_LMP": {
                "url": "PRC_RTPD_LMP&version=3&startdatetime={start_date}&enddatetime={end_date}&market_run_id=RTPD&grp_type=ALL",
                "path": os.path.join(self.datadir, "caiso_node_fmm_lmp"),
                "frequency": "hourly",
            },
            "PRC_LMP_DAM": {
                "url": "PRC_LMP&version=12&startdatetime={start_date}&enddatetime={end_date}&market_run_id=DAM&grp_type=ALL",
                "path": os.path.join(self.datadir, "caiso_node_dam_lmp"),
                "frequency": "hourly",
            },
            "PRC_INTVL_LMP_RTM": {
                "url": "PRC_INTVL_LMP&version=3&startdatetime={start_date}&enddatetime={end_date}&market_run_id=RTM&grp_type=ALL",
                "path": os.path.join(self.datadir, "caiso_node_rtm_lmp"),
                "frequency": "hourly",
            },
            "PRC_AS": {
                "url": "PRC_AS&version=12&startdatetime={start_date}&enddatetime={end_date}&market_run_id=DAM&anc_type=ALL&anc_region=ALL",
                "path": os.path.join(
                    self.datadir, "caiso_as_dam_regup_regdown_spin_nonspin"
                ),
                "frequency": "hourly",
            },
        }

    def fetchurl(self, urlstr: str):
        header = {
            "Accept": "text/javascript, application/javascript, application/ecmascript, application/x-ecmascript, */*; q=0.01",
            "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
            "Host": "oasis.caiso.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
            "Referer": "http://oasis.caiso.com/mrioasis/logon.do?reason=application.baseAction.noSession",
            "Connection": "keep-alive",
            "X-Requested-With": "XMLHttpRequest",
        }

        try:
            response = requests.get(urlstr, headers=header, timeout=20)
            contents = response.content
        except requests.exceptions.ConnectionError:
            return None, None, None
        try:
            fname = re.findall(
                "filename=(.+)", response.headers["Content-Disposition"]
            )[0]
            fname = re.sub(";", "", fname)
        except KeyError:
            return None, None, None

        return contents, response.status_code, fname

    def fetchurl_to_file(self, urlstr: str, target: str):
        contents, status_code, filename = self.fetchurl(urlstr)
        if contents is None or status_code != 200:
            return False, None

        if not os.path.exists(target):
            os.makedirs(target)

        target = os.path.join(target, filename)
        if os.path.exists(target):
            return False, target
        with open(target, "wb") as fhandle:
            fhandle.write(contents)

        if os.path.getsize(target) == 0:
            os.remove(target)

        return True, target

    def download_report(self, report_type, start_date, end_date, target_dir):
        report = self.REPORTS[report_type]

        url = self.baseurl + report["url"].format(
            start_date=start_date, end_date=end_date
        )
        return self.fetchurl_to_file(url, target_dir)

    def verify_downloads(self, path, report):
        data = []
        available_data_dates = []
        files = os.listdir(path)
        files.sort()
        if ".DS_Store" in files:
            files.remove(".DS_Store")
        if len(files) == 0:
            return {}, []
        startdate = files[0].split("_")[0]
        enddate = files[-1].split("_")[0]
        date_ranges = self.get_date_ranges(startdate, enddate)
        for file in files:
            _res = {}
            date = file.split("_")[0]
            if date not in data:
                fordate = datetime.strptime(date, "%Y%m%d").strftime(
                    "%Y-%m-%d"
                )
                if "GRP_N_N" not in file:
                    hourly_files = [f for f in files if date in f]
                    _res = {
                        "report": report,
                        "fordate": fordate,
                        "total_files": len(hourly_files),
                    }
                else:
                    daily_files = [f for f in files if date in f]
                    _res = {
                        "report": report,
                        "fordate": fordate,
                        "total_files": len(daily_files),
                    }
                data.append(_res)
                available_data_dates.append(fordate)
        missing_data = [
            {"report": report, "missing_dates": d}
            for d in date_ranges
            if d not in available_data_dates
        ]

        return data, missing_data

    def get_date_ranges(self, start, end):
        start = datetime.strptime(start, "%Y%m%d")
        end = datetime.strptime(end, "%Y%m%d")
        r = (end + timedelta(days=1) - start).days
        return [
            (start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(r)
        ]


class CaisoDownloads(object):
    def __init__(self, data_dir):
        self.datadir = data_dir
        self.report_type = None
        self.attempt = 0
        logfile_path = os.path.join(
            "/home/fox/Desktop/dir_str_as_per_badger/scripts/caiso", "log.txt"
        )

        # Ensure that the directory exists before creating the log file
        log_dir = os.path.dirname(logfile_path)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        logging.basicConfig(filename=logfile_path, level=logging.INFO)

    def verify_download(self, report, status, filename, date):
        """
        This method checks if the download was successful and downloaded file is valid
        :param report: report type
        :param status: status of the download
        :param filename: downloaded file name
        :param date: date of the download
        :return:
            date: date for next download
        """
        if filename:
            if ".xml" in filename:
                if self.attempt < 5:
                    print(
                        f"Rerunning {self.report_type} data, date {date} for the attempt: {self.attempt}."
                    )
                    print(
                        "Because GroupZip DownLoad is in Processing and didn't receive the report yet!\n"
                    )

                    date += (
                        timedelta(days=1)
                        if report["frequency"] == "daily"
                        else timedelta(hours=1)
                    )
                    os.remove(filename)
                    self.attempt += 1
                    # time.sleep(5)
                else:
                    print(
                        f"Failed to download {self.report_type} data, date: {date}\n"
                    )
                    self.attempt = 0
            elif status:
                print(
                    f"Downloaded {self.report_type} data, filename: {os.path.basename(filename)}\n"
                )
                self.attempt = 0
            else:
                print(
                    f"File already exists, skipping download for filename: {os.path.basename(filename)}\n"
                )
        else:
            print(
                f"Error downloading {self.report_type} data, for date {date}\n"
            )
            logging.error(
                "Error downloading %s data, for date %s",
                self.report_type,
                date,
            )
        return date

    def download(
        self, report_type, startdate, enddate=None, one_off_task=False
    ):
        obj = CaisoScraper(self.datadir)
        date = datetime.strptime(enddate, "%Y-%m-%d %H")
        tz = pytz.timezone(obj.PACIFIC_TIMEZONE)
        date = tz.localize(date)
        date = date.astimezone(timez.utc)
        self.report_type = report_type
        if enddate is None:
            enddate = datetime.now().strftime("%Y-%m-%d %H")
        enddate = tz.localize(
            datetime.strptime(enddate, "%Y-%m-%d %H")
        ).astimezone(timez.utc)
        startdate = tz.localize(
            datetime.strptime(startdate, "%Y-%m-%d %H")
        ).astimezone(timez.utc)
        report = obj.REPORTS[report_type]
        if report["frequency"] == "daily":
            delta = timedelta(days=1)
        else:
            delta = timedelta(hours=1)

        count = 0

        while date >= startdate:
            start_date = datetime.strftime(date, "%Y%m%dT%H:%M%z").replace(
                "+", "-"
            )
            date -= delta
            end_date = datetime.strftime(
                (date + delta + delta), "%Y%m%dT%H:%M%z"
            ).replace("+", "-")

            print(".", end="")

            foo = (date + delta).astimezone(tz)
            filename = f"/data/caiso/FMM Locational Marginal Prices (LMP)/{foo.strftime('%Y%m%d')}*_{foo.hour + 1:02d}_*csv.zip"
            found = glob.glob(filename)
            if found:
                continue

            # Directly use the report path
            target_path = report["path"]

            count += 1
            d_status, filename = obj.download_report(
                report_type, start_date, end_date, target_path
            )
            if filename:
                date = self.verify_download(report, d_status, filename, date)

            if one_off_task:
                break
            time.sleep(5)

        print("\n", count)


if __name__ == "__main__":
    from datetime import date

    ref_date = date.today()
    print("Today's date is: ", ref_date)

    report_dates = {
        "PRC_RTPD_LMP": ("2024-06-25 00", "2024-06-26 00"),
        "PRC_INTVL_AS": ("2024-06-24 00", "2024-06-25 00"),
        "PRC_LMP_DAM": ("2024-06-23 00", "2024-06-24 00"),
        "PRC_INTVL_LMP_RTM": ("2024-06-22 00", "2024-06-23 00"),
        "PRC_AS": ("2024-06-21 00", "2024-06-22 00"),
    }

    base_path = (
        f"/home/fox/Desktop/july_onwards_daily_reports/daily_data/{ref_date}"
    )

    data_paths = {
        "PRC_RTPD_LMP": base_path,
        "PRC_INTVL_AS": base_path,
        "PRC_LMP_DAM": base_path,
        "PRC_INTVL_LMP_RTM": base_path,
        "PRC_AS": base_path,
    }

    processes = []
    for price_type, (start_date, end_date) in report_dates.items():
        data_path = data_paths[price_type]
        obj = CaisoDownloads(data_path)
        process = multiprocessing.Process(
            target=obj.download,
            args=(price_type, start_date, end_date, False),
        )
        processes.append(process)

    for process in processes:
        process.start()

    for process in processes:
        process.join()
