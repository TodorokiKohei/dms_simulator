import argparse
import os
import time

import yaml

from libs.kafka import KafkaController
from libs.swarm_executer import SwarmExecuter
from libs.utils import Node, NodeManager


class DmsSimulator():
    def __init__(self, controller):
        self._controller = controller

    def run(self):
        # 初期設定
        print('############### Initializing ############################')
        self._controller.initialize()

        # コンテナ展開
        print('############### Deploying ############################')
        self._controller.deploy_broker()
        time.sleep(10)

        # 環境を掃除する
        print('############### Cleaning ############################')
        self._controller.clean()


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
