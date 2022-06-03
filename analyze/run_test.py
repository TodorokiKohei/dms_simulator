import glob
import os
import subprocess

import yaml


def run_test(broker, template_file, res_suffix):
    proc = subprocess.run(f"bash run_test.sh '{template_file}' '{broker}' '{res_suffix}'", shell=True, text=True)
    

def test_kafka(broker, template_file, pub_conf_list, message_size):
    for acks in ["0", "1"]:
        for pub_conf_file in pub_conf_list:
            with open(pub_conf_file, "r") as f:
                pub_conf = yaml.safe_load(f)
                pub_conf["pubConf"]['messageSize'] = message_size
                pub_conf["pubConf"]['properties']['acks'] = acks
            with open(pub_conf_file, "w") as f:
                yaml.safe_dump(pub_conf, f, sort_keys=False)
        run_test(broker, template_file, "size_" + message_size + "_acks_" + acks)

def test_jetstream(broker, template_file, pub_conf_list, message_size):
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    run_test(broker, template_file, "size_" + message_size)

def test_nats(broker, template_file, pub_conf_list, message_size):
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    run_test(broker, template_file, "size_" + message_size)

# broker_list = ["kafka", "jetstream", "nats"]
broker_list = ["kafka"]
for message_size in ["100b", "1kb", "4kb", "8kb", "16kb"]:
    for broker in broker_list:
        template_file = os.path.join("templates", f"template_{broker}.yml")
        pub_conf_list = glob.glob(os.path.join("..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
        if broker == "kafka":
            test_kafka(broker, template_file, pub_conf_list, message_size)
        elif broker == "jetstream":
            test_jetstream(broker, template_file, pub_conf_list, message_size)
        elif broker == "nats":
            test_nats(broker, template_file, pub_conf_list, message_size)