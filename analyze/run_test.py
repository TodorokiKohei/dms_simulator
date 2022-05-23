import copy
import subprocess
from datetime import datetime

import yaml

with open("template.yml", mode='r', encoding='utf-8') as f:
        template = yaml.safe_load(f)

# for acks in [1, "all"]:
for acks in [1]:
    pub_1 = template["systems"]["publisher"]["pub-1"]
    pub_1["params"]["acks"] = acks

    # for delay_time in [2, 5, 10, 50, 100]:
    for delay_time in [150, 200, 250]:
        template_test = copy.deepcopy(template)
        template_test["actions"].pop("loss1")
        if delay_time == 0:
            template_test.pop("actions")
        else:
            template_test["actions"]["delay1"]["params"]["time"] = delay_time

        with open("template_test.yml", "w") as f:
            yaml.safe_dump(template_test, f, sort_keys=False)

        result_file = datetime.now().strftime("%Y%m%d_%H%M%S") + "_acks_" + str(acks) + "_delay_" + str(delay_time)
        proc = subprocess.run(f"bash run_test.sh '{result_file}'", shell=True)

    # for loss_per in [0.1, 0.5, 1]:
    for loss_per in [2, 3, 4]:
        template_test = copy.deepcopy(template)
        template_test["actions"].pop("delay1")
        template_test["actions"]["loss1"]["params"]["percent"] = loss_per

        with open("template_test.yml", "w") as f:
            yaml.safe_dump(template_test, f, sort_keys=False)

        result_file = datetime.now().strftime("%Y%m%d_%H%M%S") + "_acks_" + str(acks) + "_loss_" + str(loss_per)
        proc = subprocess.run(f"bash run_test.sh '{result_file}'", shell=True)
