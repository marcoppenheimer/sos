from datetime import datetime, timedelta
import json
from sos.report.plugins import Plugin, UbuntuPlugin, PluginOpt
import glob
import re

PATHS = {
    "CONF": f"/var/snap/charmed-kafka/current/etc/cruise-control",
    "LOGS": f"/var/snap/charmed-kafka/common/var/log/cruise-control",
}

DATE_FORMAT = "%Y-%m-%d-%H"


class CharmedCruiseControl(Plugin, UbuntuPlugin):

    short_desc = "Cruise Control (from Charmed Kafka)"
    plugin_name = "charmed_cruise_control"

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

    @property
    def credentials_args(self) -> str:
        with open(f"{PATHS['CONF']}/cruisecontrol.credentials") as f:
            content = f.read().strip()

        if match := re.match(r"balancer: (?P<pwd>\w+),ADMIN", content):
            pwd = match.group("pwd")
            return f"-u balancer:{pwd}"
        else:
            return ""

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
                f"{PATHS['CONF']}/*.key",
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
            "snap logs charmed-kafka.cruise-control -n 100000",
            suggest_filename="snap-logs",
        )

        # --- CRUISE CONTROL STATE ---

        self.add_cmd_output(
            f"curl {self.credentials_args} localhost:9090/kafkacruisecontrol/state?super_verbose=true",
            "cruise-control-state",
        )

        # --- CLUSTER STATE ---

        self.add_cmd_output(
            f"curl {self.credentials_args} localhost:9090/kafkacruisecontrol/kafka_cluster_state?verbose=true",
            "cluster-state",
        )

        # --- PARTITION LOAD ---

        self.add_cmd_output(
            f"curl {self.credentials_args} localhost:9090/kafkacruisecontrol/partition_load",
            "partition-load",
        )

        # --- USER TASKS ---

        self.add_cmd_output(
            f"curl {self.credentials_args} localhost:9090/kafkacruisecontrol/user_tasks",
            "user-tasks",
        )

        # --- JMX METRICS ---

        self.add_cmd_output("curl localhost:9102/metrics", "jmx-metrics")

    def postproc(self):
        # --- SCRUB PASSWORDS ---

        self.do_path_regex_sub(
            f"{PATHS['CONF']}/*",
            r'(password=")[^"]*',
            r"\1*********",
        )

        self.do_path_regex_sub(
            f"{PATHS['CONF']}/*",
            r"(balancer: )[^,]*",
            r"\1*********",
        )
