from datetime import datetime, timedelta
import json
from sos.report.plugins import Plugin, UbuntuPlugin, PluginOpt
import glob
import re

PATHS = {
    "CONF": "/var/snap/charmed-kafka/current/etc/kafka",
    "LOGS": "/var/snap/charmed-kafka/common/var/log/kafka",
}

DATE_FORMAT = "%Y-%m-%d-%H"


class CharmedKafka(Plugin, UbuntuPlugin):

    short_desc = "Charmed Kafka"
    plugin_name = "charmed_kafka"

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
    def bootstrap_server(self) -> str | None:
        lines = []
        with open(f"{PATHS['CONF']}/client.properties") as f:
            lines = f.readlines()

        for line in lines:
            if "bootstrap" in line:
                return line.split("=")[1]

    @property
    def default_bin_args(self) -> str:
        if not self.bootstrap_server:
            return ""

        return f"--bootstrap-server {self.bootstrap_server} --command-config {PATHS['CONF']}/client.properties"

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
            "snap logs charmed-kafka.daemon -n 100000", suggest_filename="snap-logs"
        )

        # --- CONFIGS ---

        for entity in ["topics", "clients", "users", "brokers", "ips"]:
            self.add_cmd_output(
                f"charmed-kafka.configs --describe --all --entity-type {entity} {self.default_bin_args}",
                env={"KAFKA_OPTS": ""},
                suggest_filename=f"kafka-configs-{entity}",
            )

        # --- ACLs ---

        self.add_cmd_output(
            f"charmed-kafka.acls --list {self.default_bin_args}",
            env={"KAFKA_OPTS": ""},
            suggest_filename="kafka-acls",
        )

        # --- LOG DIRS ---

        log_dirs_output = self.exec_cmd(
            f"charmed-kafka.log-dirs --describe {self.default_bin_args}",
            env={"KAFKA_OPTS": ""},
        )
        log_dirs = {}

        if log_dirs_output and log_dirs_output["status"] == 0:
            for line in log_dirs_output["output"].splitlines():
                try:
                    log_dirs = json.loads(line)
                    break
                except json.JSONDecodeError:
                    continue

        with self.collection_file("kafka-log-dirs") as f:
            f.write(json.dumps(log_dirs, indent=4))

        # --- TRANSACTIONS ---

        transactions_list = self.exec_cmd(
            f"charmed-kafka.transactions {self.default_bin_args} list",
            env={"KAFKA_OPTS": ""},
        )
        transactional_ids = []

        if transactions_list and transactions_list["status"] == 0:
            transactional_ids = transactions_list["output"].splitlines()[1:]

        transactions_outputs = []
        for transactional_id in transactional_ids:
            transactions_describe = self.exec_cmd(
                f"charmed-kafka.transactions {self.default_bin_args} describe --transactional-id {transactional_id}"
            )

            if transactions_describe and transactions_describe["status"] == 0:
                transactions_outputs.append(transactions_describe["output"])

        with self.collection_file("kafka-transactions") as f:
            f.write("\n".join(transactions_outputs))

        # --- JMX METRICS ---

        self.add_cmd_output("curl localhost:9101/metrics", "jmx-metrics")

    def postproc(self):
        # --- SCRUB PASSWORDS ---

        self.do_path_regex_sub(
            f"{PATHS['CONF']}/*",
            r'(password=")[^"]*',
            r"\1*********",
        )
