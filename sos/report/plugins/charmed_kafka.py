import json
from sos.report.plugins import Plugin, UbuntuPlugin

PATHS = {
    "CONF": f"/var/snap/charmed-kafka/current/etc/kafka",
    "LOGS": f"/var/snap/charmed-kafka/common/var/log/kafka",
}


class CharmedKafka(Plugin, UbuntuPlugin):

    short_desc = "Charmed Kafka"
    plugin_name = "charmed_kafka"

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
        # --- FILE INCLUSIONS ---

        self.add_copy_spec(
            [
                f"{PATHS['CONF']}",
                f"{PATHS['LOGS']}",
                "/var/log/juju",
            ]
        )

        # --- FILE EXCLUSIONS ---

        self.add_forbidden_path(
            [
                f"{PATHS['CONF']}/*.pem",
                f"{PATHS['CONF']}/*.p12",
                f"{PATHS['CONF']}/*.jks",
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
