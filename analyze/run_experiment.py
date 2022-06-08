import glob
import os
import subprocess
import re

import yaml


def run_single(broker, template_file, res_suffix):
    proc = subprocess.run(
        f"bash run_all.sh '{template_file}' '{broker}' '{res_suffix}'", shell=True, text=True)

def run_deploy(template_file):
    proc = subprocess.run(
        f"bash run_deploy.sh '{template_file}'", shell=True, text=True, capture_output=True)
    return proc.stdout
    
def run_test(broker, template_file, res_suffix):
    proc = subprocess.run(
        f"bash run_test.sh '{template_file}' '{broker}' '{res_suffix}'", shell=True, text=True)

def test_kafka_cl(broker, template_file, pub_conf_list, message_size, res_suffix):
    for acks in ["0", "1"]:
        deploy_res = run_deploy(template_file)
        topic_info = deploy_res.split('\n')[-2]
        leader = topic_info.split('\\t')[3].split(' ')[-1]
        for pub_conf_file in pub_conf_list:
            with open(pub_conf_file, "r") as f:
                pub_conf = yaml.safe_load(f)
                pub_conf["pubConf"]['messageSize'] = message_size
                pub_conf["pubConf"]['properties']['acks'] = acks
            with open(pub_conf_file, "w") as f:
                yaml.safe_dump(pub_conf, f, sort_keys=False)
        run_test(broker, template_file, "size_"+message_size+"_acks_"+acks+res_suffix)
        

def test_jetstream_cl(broker, template_file, pub_conf_list, sub_conf_list, message_size, res_suffix):
    deploy_res = run_deploy(template_file)
    cluster_info = [s for s in deploy_res.split('\n') if re.match(".*leader.*", s)]
    leader = re.split('[-|"]', cluster_info[0])[4]
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
            pub_conf["pubConf"]['server'] = 'nats://jetstream-' + leader
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    for sub_conf_file in sub_conf_list:
        with open(sub_conf_file, "r") as f:
            sub_conf = yaml.safe_load(f)
            sub_conf["subConf"]['server'] = 'nats://jetstream-' + leader
        with open(sub_conf_file, "w") as f:
            yaml.safe_dump(sub_conf, f, sort_keys=False)
    run_test(broker, template_file, "size_"+message_size+res_suffix)


def test_kafka(broker, template_file, pub_conf_list, message_size, res_suffix):
    for acks in ["0", "1"]:
        for pub_conf_file in pub_conf_list:
            with open(pub_conf_file, "r") as f:
                pub_conf = yaml.safe_load(f)
                pub_conf["pubConf"]['messageSize'] = message_size
                pub_conf["pubConf"]['properties']['acks'] = acks
            with open(pub_conf_file, "w") as f:
                yaml.safe_dump(pub_conf, f, sort_keys=False)
        run_single(broker, template_file, "size_" +
                 message_size + "_acks_" + acks + res_suffix)


def test_jetstream(broker, template_file, pub_conf_list, message_size, res_suffix):
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    run_single(broker, template_file, "size_" + message_size, res_suffix)


def test_nats(broker, template_file, pub_conf_list, message_size, res_suffix):
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    run_single(broker, template_file, "size_" + message_size, res_suffix)


if __name__ == '__main__':
    isCluster = True
    res_suffix = '_cl_3'

    if isCluster:
        # broker_list = ["kafka", "jetstream"]
        broker_list = ["jetstream", "kafka"]
        for message_size in ["100b", "1kb", "4kb", "8kb"]:
        # for message_size in ["100b"]:
            for broker in broker_list:
                template_file = os.path.join("templates", f"template_{broker}_cl.yml")
                pub_conf_list = glob.glob(os.path.join(
                    "..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
                sub_conf_list = glob.glob(os.path.join(
                    "..", "exec_dir", "controller", broker, "*sub*", "configs", "*.yaml"))

                if broker == "kafka":
                    test_kafka_cl(broker, template_file, pub_conf_list, message_size, res_suffix)
                elif broker == "jetstream":
                    test_jetstream_cl(broker, template_file, pub_conf_list, sub_conf_list, message_size, res_suffix)

    else:
        broker_list = ["kafka", "jetstream", "nats"]
        # broker_list = ["kafka"]
        for message_size in ["100b", "1kb", "4kb", "8kb", "16kb"]:
            for broker in broker_list:
                template_file = os.path.join("templates", f"template_{broker}.yml")
                pub_conf_list = glob.glob(os.path.join(
                    "..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
                if broker == "kafka":
                    test_kafka(broker, template_file, pub_conf_list, message_size, res_suffix)
                elif broker == "jetstream":
                    test_jetstream(broker, template_file, pub_conf_list, message_size, res_suffix)
                elif broker == "nats":
                    test_nats(broker, template_file, pub_conf_list, message_size, res_suffix)
