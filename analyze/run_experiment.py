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
    stdout = proc.stdout
    print(stdout)
    return stdout


def run_test(broker, template_file, res_suffix):
    proc = subprocess.run(
        f"bash run_test.sh '{template_file}' '{broker}' '{res_suffix}'", shell=True, text=True)

###########################################################################################################

def create_fault_test_template(broker, template_file, leader, delay):
    with open(os.path.join("..", template_file)) as f:
        template = yaml.safe_load(f)
    template['actions']['delay1']['params']['time'] = delay
    template['actions']['delay1']['target_containers'] = []
    template['actions']['delay1']['destination'] = []
    cnt = 0
    for i in range(1, 4):
        if str(i) == leader:
            template['actions']['delay1']['target_containers'].append(f"{broker}-{i}")
        else:
            cnt += 1
            if cnt == 1:
                template['actions']['delay1']['destination'].append(f"{broker}-{i}")
    test_template_file = os.path.join(os.path.dirname(template_file), 'test_'+os.path.basename(template_file))
    with open(os.path.join('../', test_template_file), mode='w') as f:
        yaml.safe_dump(template, f, sort_keys=False)

    return test_template_file

def test_kafka_cl_delay(broker, template_file, pub_conf_list, message_size, acks, res_suffix, delay, message_rate):
    deploy_res = run_deploy(template_file)
    topic_info = deploy_res.split('\n')[-2]
    leader = topic_info.split('\\t')[3].split(' ')[-1]
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
            pub_conf["pubConf"]['messageRate'] = message_rate
            pub_conf["pubConf"]['properties']['acks'] = acks
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)

    test_template_file = create_fault_test_template(broker, template_file, leader, delay)
    run_test(broker, test_template_file, "size_" +
             message_size+"_acks_"+acks+res_suffix)


def test_jetstream_cl_delay(broker, template_file, pub_conf_list, sub_conf_list, message_size, res_suffix, delay, message_rate):
    deploy_res = run_deploy(template_file)
    cluster_info = [s for s in deploy_res.split(
        '\n') if re.match(".*leader.*", s)]
    leader = re.split('[-|"]', cluster_info[0])[4]
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
            pub_conf["pubConf"]['messageRate'] = message_rate
            pub_conf["pubConf"]['server'] = 'nats://jetstream-' + leader
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    for sub_conf_file in sub_conf_list:
        with open(sub_conf_file, "r") as f:
            sub_conf = yaml.safe_load(f)
            sub_conf["subConf"]['server'] = 'nats://jetstream-' + leader
        with open(sub_conf_file, "w") as f:
            yaml.safe_dump(sub_conf, f, sort_keys=False)

    test_template_file = create_fault_test_template(broker, template_file, leader, delay)
    run_test(broker, test_template_file, "size_"+message_size+res_suffix)

###########################################################################################################


def test_kafka_cl(broker, template_file, pub_conf_list, message_size, acks, res_suffix):
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
    run_test(broker, template_file, "size_" +
             message_size+"_acks_"+acks+res_suffix)


def test_jetstream_cl(broker, template_file, pub_conf_list, sub_conf_list, message_size, res_suffix):
    deploy_res = run_deploy(template_file)
    cluster_info = [s for s in deploy_res.split(
        '\n') if re.match(".*leader.*", s)]
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

###########################################################################################################


def test_kafka(broker, template_file, pub_conf_list, message_size, acks, res_suffix):
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
    run_single(broker, template_file, "size_" + message_size+res_suffix)


def test_nats(broker, template_file, pub_conf_list, message_size, res_suffix):
    for pub_conf_file in pub_conf_list:
        with open(pub_conf_file, "r") as f:
            pub_conf = yaml.safe_load(f)
            pub_conf["pubConf"]['messageSize'] = message_size
        with open(pub_conf_file, "w") as f:
            yaml.safe_dump(pub_conf, f, sort_keys=False)
    run_single(broker, template_file, "size_" + message_size+res_suffix)

###########################################################################################################


if __name__ == '__main__':
    # isCluster = False
    # isFault = False
    # res_suffix = ''
    # isCluster = True
    # isFault = False
    # res_suffix = '_cl_3'
    isCluster = True
    isFault = True
    res_suffix = '_cl_3_fault'

    if isCluster and isFault:
        broker_list = ["kafka", "jetstream"]
        # broker_list = ["jetstream"]
        delay = 20
        message_size = '4kb'
        print("Test Cluster and Fault!!! Broker List: "+", ".join(broker_list))
        for broker in broker_list:
            template_file = os.path.join(
                "templates", f"template_{broker}_cl_delay.yml")
            pub_conf_list = glob.glob(os.path.join(
                "..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
            sub_conf_list = glob.glob(os.path.join(
                "..", "exec_dir", "controller", broker, "*sub*", "configs", "*.yaml"))
            if broker == "kafka":
                acks = '1'
                message_rate = '10mb'
                test_kafka_cl_delay(broker, template_file, pub_conf_list,
                                    message_size, acks, res_suffix, delay, message_rate)
                acks = 'all'
                message_rate = '3mb'
                test_kafka_cl_delay(broker, template_file, pub_conf_list,
                                    message_size, acks, res_suffix, delay, message_rate)
            elif broker == "jetstream":
                message_rate = '10mb'
                test_jetstream_cl_delay(broker, template_file, pub_conf_list,
                                        sub_conf_list, message_size, res_suffix, delay, message_rate)

    elif isCluster:
        # broker_list = ["jetstream", "kafka"]
        broker_list = ["jetstream"]
        print("Test Cluster !!! Broker List: "+", ".join(broker_list))
        # for message_size in ["100b", "1kb", "4kb", "8kb"]:
        # for message_size in ["16kb", "32kb", "64kb"]:
        for message_size in ["8kb"]:
            for broker in broker_list:
                template_file = os.path.join(
                    "templates", f"template_{broker}_cl.yml")
                pub_conf_list = glob.glob(os.path.join(
                    "..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
                sub_conf_list = glob.glob(os.path.join(
                    "..", "exec_dir", "controller", broker, "*sub*", "configs", "*.yaml"))
                if broker == "kafka":
                    for acks in ["1", "all"]:
                        test_kafka_cl(
                            broker, template_file, pub_conf_list, message_size, acks, res_suffix)
                elif broker == "jetstream":
                    test_jetstream_cl(
                        broker, template_file, pub_conf_list, sub_conf_list, message_size, res_suffix)

    else:
        # broker_list = ["kafka", "jetstream", "nats"]
        broker_list = ["kafka"]
        print("Test Single !!! Broker List:"+",".join(broker_list))
        # for message_size in ["100b", "1kb", "4kb", "8kb", "16kb"]:
        for message_size in ["8kb"]:
            for broker in broker_list:
                template_file = os.path.join(
                    "templates", f"template_{broker}.yml")
                pub_conf_list = glob.glob(os.path.join(
                    "..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
                if broker == "kafka":
                    for acks in ["0", "1", "all"]:
                        test_kafka(broker, template_file, pub_conf_list,
                                   message_size, acks, res_suffix)
                elif broker == "jetstream":
                    test_jetstream(broker, template_file,
                                   pub_conf_list, message_size, res_suffix)
                elif broker == "nats":
                    test_nats(broker, template_file, pub_conf_list,
                              message_size, res_suffix)
