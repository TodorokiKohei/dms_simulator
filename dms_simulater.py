import argparse
import os
import time
import threading

import schedule
import yaml

from libs.kafka import KafkaController
from libs.swarm_executer import SwarmExecuter
from libs.utils import Node, NodeManager


class DmsSimulator():
    def __init__(self, controller):
        self._controller = controller
        self._scheduler = schedule.Scheduler()

    def initialize(self):
        """
        """
        # 初期設定
        print('############### Initializing ###############')
        self._controller.initialize()

    def _check_container_running(self, check_function, nums):
        """
        コンテナサービスが起動しているかどうかを確認する
        """
        for _ in range(nums):
            container_status = check_function()
            ng_container = []
            all_up = True
            for container_name, status in container_status.items():
                if not status:
                    ng_container.append(container_name)
                    all_up = False
            if all_up:
                return
            time.sleep(1)
        raise RuntimeError(
            f'Container [{",".join(ng_container)}] is not running properly.')

    def _check_container_down(self, check_function):
        """
        コンテナサービスが停止しているかどうかを確認する
        """
        container_status = check_function()
        all_down = True
        for _, status in container_status.items():
            if status:
                all_down = False
        return all_down

    def deploy(self):
        """
        """
        # Brokerを展開し、全て起動しているかどうかを確認する
        print('############### Deploying ###############')
        self._controller.deploy_broker()
        self._check_container_running(self._controller.check_broker_running, 5)

    def _collect_job(self):
        """
        """
        print('############### Collect results ###############')
        while True:
            if self._check_container_down(self._controller.check_subscriber_running):
                break
            time.sleep(1)
        while True:
            if self._check_container_down(self._controller.check_publisher_running):
                break
            time.sleep(1)
        self._controller.collect_container_results()
        return schedule.CancelJob

    def test_performance(self):
        """
        """
        # pulibhser,subscriberを展開し、パフォーマンスのテストを行う
        print('############### Test perfomance ###############')
        thread_pub = threading.Thread(
            target=self._controller.deploy_subscriber)
        thread_sub = threading.Thread(target=self._controller.deploy_publisher)
        thread_pub.start()
        thread_sub.start()
        thread_pub.join()
        thread_sub.join()

        self._check_container_running(
            self._controller.check_subscriber_running, 5)
        self._check_container_running(
            self._controller.check_publisher_running, 5)

        print(f"-------------- Execution Time: {controller.duration} sec --------------")

        # 実行結果の回収時間をスケジューラーに登録し待機する
        self._scheduler.every(
            self._controller.duration).seconds.do(self._collect_job)
        while self._scheduler.get_jobs() != []:
            self._scheduler.run_pending()
            time.sleep(1)

        # publisher, subscriberを削除
        self._controller.remove_publisher()
        self._controller.remove_subscriber()

    def clean(self):
        """
        """
        # 環境を掃除する
        print('############### Cleaning ###############')
        self._controller.remove_broker()
        self._controller.clean()

    def run(self):
        """
        """
        self.initialize()
        self.deploy()
        self.test_performance()
        self.clean()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', default='run', choices=['run', 'init', 'deploy', 'test', 'clean'],
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
    if keyfile is None:
        raise RuntimeError('Please specify keyfile for ssh.')
    node_manager = NodeManager()
    for name, configs in template['Base']['Nodes'].items():
        node = Node(name, user, keyfile, sudopass, **configs)
        node_manager.append(node)

    # ノード情報とコンテナ情報を持つコントローラーを作成
    root_dir = 'exec_dir'
    executer = SwarmExecuter(home_dir=os.path.join(
        root_dir, 'executer'), remote_dir='/tmp/exec_dir', debug_mode=args.debug)
    if template["Systems"]["type"] == 'kafka':
        controller = KafkaController(
            node_manager, executer, template['Systems'], root_dir)
    controller.init_containers()

    # シミュレーションにコントローラーを設定し実行する
    ds = DmsSimulator(controller)
    if args.mode == 'run':
        ds.run()
    elif args.mode == 'init':
        ds.initialize()
    elif args.mode == 'deploy':
        ds.deploy()
    elif args.mode == 'test':
        ds.test_performance()
    elif args.mode == 'clean':
        ds.clean()
