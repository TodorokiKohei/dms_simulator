import argparse

import yaml

from libs.executers import SwarmExecuter
from libs.controllers import KafkaController


class DmsSimulator():
    def __init__(self, controller):
        pass

    def run(self):
        pass


if __name__ == "__main__":
    with open("template.yml", mode='r', encoding='utf-8') as f:
        template = yaml.safe_load(f)

    executer = SwarmExecuter(template['Base'])
    executer.init()

    if template["Systems"]["type"]:
        controller = KafkaController(executer, template['Systems'])

    ds = DmsSimulator(controller)
    ds.run()
