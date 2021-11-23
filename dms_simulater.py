import argparse
import os

import yaml

from libs.controllers import KafkaController
from libs.executers import SwarmExecuter
from libs.utils import Node, NodeManager


class DmsSimulator():
    def __init__(self, controller):
        self.controller = controller

    def run(self):
        # 初期設定
        print('############### Initializing ############################')
        self.controller.init()        

        # コンテナ展開
        # print('############### Deploying ############################')
        # self.controller.deploy_broker()

        # 
        print('############### Cleaning ############################')
        self.controller.remove()
        self.controller.clean()


if __name__ == "__main__":
    with open("template.yml", mode='r', encoding='utf-8') as f:
        template = yaml.safe_load(f)

    # ノード情報作成
    node_manager = NodeManager()
    user = template['Base']['User']
    if template['Base']['Keyfile'][0] == '$':
        keyfile = os.environ.get(template['Base']['Keyfile'][1:])
    else:
        keyfile = template['Base']['Keyfile']
    if template['Base']['Sudopass'][0] == '$':
        sudopass = os.environ.get(template['Base']['Sudopass'][1:])
    else:
        sudopass = template['Base']['Sudopass']
    for name, configs in template['Base']['Nodes'].items():
        node = Node(name, user, keyfile, sudopass, **configs)
        node_manager.append(node)

    # コントローラー作成
    executer = SwarmExecuter(home_dir='exec_dir', remote_dir='/tmp/exec_dir')
    if template["Systems"]["type"] == 'kafka':
        controller = KafkaController(
            node_manager, executer, template['Systems'])

    # シミュレーション実行
    ds = DmsSimulator(controller)
    ds.run()
