from datetime import datetime, timedelta
from sos.report.plugins import Plugin, UbuntuPlugin, PluginOpt
import glob
import re

PATHS = {
    "CONF": "/var/snap/charmed-zookeeper/current/etc/zookeeper",
    "LOGS": "/var/snap/charmed-zookeeper/common/var/log/zookeeper",
}

DATE_FORMAT = "%Y-%m-%d-%H"


class CharmedZooKeeper(Plugin, UbuntuPlugin):

    short_desc = "Charmed ZooKeeper"
    plugin_name = "charmed_zookeeper"

    current_date = datetime.now()
    default_date_from = "1970-01-01-00"
    default_date_to = (current_date + timedelta(hours=1)).strftime(DATE_FORMAT)

    option_list = [
        PluginOpt(
            "date-from",
            default="1970-01-01-00",
            desc="date from which to filter logs, in format YYYY-MM-DD-HH",
            val_type=str,
        ),
        PluginOpt(
            "date-to",
            default=default_date_to,
            desc="date to which to filter logs, in format YYYY-MM-DD-HH",
            val_type=str,
        ),
    ]

    def setup(self):
        # --- FILE EXCLUSIONS ---

        for file in glob.glob(f"{PATHS['LOGS']}/*"):
            date = re.search(
                pattern=r"([0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2})", string=file
            )

            # include files without date, aka current files
            if not date:
                continue

            file_dt = datetime.strptime(date.group(1), DATE_FORMAT)

            if file_dt < datetime.strptime(
                str(self.get_option("date-from")), DATE_FORMAT
            ) or file_dt > datetime.strptime(
                str(self.get_option("date-to")), DATE_FORMAT
            ):
                # skip files outside given range
                self.add_forbidden_path(file)

        # hide keys/stores
        self.add_forbidden_path(
            [
                f"{PATHS['CONF']}/*.pem",
                f"{PATHS['CONF']}/*.p12",
                f"{PATHS['CONF']}/*.jks",
            ]
        )

        # --- FILE INCLUSIONS ---

        self.add_copy_spec(
            [
                f"{PATHS['CONF']}",
                f"{PATHS['LOGS']}",
                "/var/log/juju",
            ]
        )

        # --- SNAP LOGS ---

        self.add_cmd_output(
            "snap logs charmed-zookeeper.daemon -n 100000", suggest_filename="snap-logs"
        )

        # TODO: Add zkSnapShotToolkit + ZKTxnLogToolkit outputs here after they're added to the Snap

        # --- JMX METRICS ---

        self.add_cmd_output("curl localhost:9998/metrics", "jmx-metrics")

        # --- PROVIDER METRICS ---

        self.add_cmd_output("curl localhost:7000/metrics", "provider-metrics")

    def postproc(self):
        # --- SCRUB PASSWORDS ---

        self.do_path_regex_sub(
            f"{PATHS['CONF']}/*",
            r'(password=")[^"]*',
            r"\1*********",
        )
