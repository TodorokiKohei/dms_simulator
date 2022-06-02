import glob
import os
import subprocess

import yaml


def run_test(broker, template_file, res_suffix):
    proc = subprocess.run(f"bash run_test.sh '{template_file}' '{broker}' '{res_suffix}'", shell=True)

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

broker_list = ["kafka", "jetstream", "nats"]
message_size = "1kb"
for broker in broker_list:
    template_file = os.path.join("templates", f"template_{broker}.yml")
    pub_conf_list = glob.glob(os.path.join("..", "exec_dir", "controller", broker, "*pub*", "configs", "*.yaml"))
    if broker == "kafka":
        test_kafka(broker, template_file, pub_conf_list, message_size)
    elif broker == "jetstream":
        test_jetstream(broker, template_file, pub_conf_list, message_size)
    elif broker == "nats":
        test_nats(broker, template_file, pub_conf_list, message_size)

# with open("template.yml", mode='r', encoding='utf-8') as f:
#         template = yaml.safe_load(f)

# # for acks in [1, "all"]:
# for acks in [1]:
#     pub_1 = template["systems"]["publisher"]["pub-1"]
#     pub_1["params"]["acks"] = acks

#     # for delay_time in [2, 5, 10, 50, 100]:
#     for delay_time in [150, 200, 250]:
#         template_test = copy.deepcopy(template)
#         template_test["actions"].pop("loss1")
#         if delay_time == 0:
#             template_test.pop("actions")
#         else:
#             template_test["actions"]["delay1"]["params"]["time"] = delay_time

#         with open("template_test.yml", "w") as f:
#             yaml.safe_dump(template_test, f, sort_keys=False)

#         result_file = datetime.now().strftime("%Y%m%d_%H%M%S") + "_acks_" + str(acks) + "_delay_" + str(delay_time)
#         proc = subprocess.run(f"bash run_test.sh '{result_file}'", shell=True)

#     # for loss_per in [0.1, 0.5, 1]:
#     for loss_per in [2, 3, 4]:
#         template_test = copy.deepcopy(template)
#         template_test["actions"].pop("delay1")
#         template_test["actions"]["loss1"]["params"]["percent"] = loss_per

#         with open("template_test.yml", "w") as f:
#             yaml.safe_dump(template_test, f, sort_keys=False)

#         result_file = datetime.now().strftime("%Y%m%d_%H%M%S") + "_acks_" + str(acks) + "_loss_" + str(loss_per)
#         proc = subprocess.run(f"bash run_test.sh '{result_file}'", shell=True)
