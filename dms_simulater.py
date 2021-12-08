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

    def initialize(self):
        # 初期設定
        print('############### Initializing ############################')
        self._controller.initialize()

    def deploy(self):
        # コンテナ展開
        print('############### Deploying ############################')
        self._controller.deploy_broker()
        self._controller.deploy_subscriber()
        self._controller.deploy_publisher()
        time.sleep(10)

    def clean(self):
        # 環境を掃除する
        print('############### Cleaning ############################')
        self._controller.clean()

    def run(self):
        self.initialize()
        self.deploy()
        self.clean()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default='run', choices=['run', 'init', 'deploy', 'clean'],
                        help='Specify the execution mode (default: run the test)')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    with open("template.yml", mode='r', encoding='utf-8') as f:
        template = yaml.safe_load(f)

    # ノード情報作成
    user = template['Base']['User']
    if template['Base']['Keyfile'][0] == '$':
        keyfile = os.environ.get(template['Base']['Keyfile'][1:])
    else:
        keyfile = template['Base']['Keyfile']
    if template['Base']['Sudopass'][0] == '$':
        sudopass = os.environ.get(template['Base']['Sudopass'][1:])
    else:
        sudopass = template['Base']['Sudopass']
    node_manager = NodeManager()
    for name, configs in template['Base']['Nodes'].items():
        node = Node(name, user, keyfile, sudopass, **configs)
        node_manager.append(node)

    # ノード情報とコンテナ情報を持つコントローラーを作成
    root_dir = 'exec_dir'
    executer = SwarmExecuter(home_dir=os.path.join(root_dir, 'executer'), remote_dir='/tmp/exec_dir', debug_mode=args.debug)
    if template["Systems"]["type"] == 'kafka':
        controller = KafkaController(
            node_manager, executer, template['Systems'], root_dir)
    controller.init_container()

    # シミュレーションにコントローラーを設定し実行する
    ds = DmsSimulator(controller)
    if args.mode == 'run':
        ds.run()
    elif args.mode == 'init':
        ds.initialize()
    elif args.mode == 'deploy':
        ds.deploy()
    elif args.mode == 'clean':
        ds.clean()
