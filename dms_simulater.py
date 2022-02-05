import argparse
import os
import threading
import time

import schedule
import yaml

from libs.base.controllers import Controller
from libs.kafka import KafkaController
from libs.swarm_executer import SwarmExecuter
from libs.utils import Node, NodeManager


class DmsSimulator():
    def __init__(self, controller:Controller):
        self._controller = controller
        self._scheduler = schedule.Scheduler()
        self._check_count = 10
        self._exec_info = None

    def open(self):
        self._io = open("execute_time.log", mode="a")
        self._io.write(f"--------------------------------------------------\n")

    def close(self):
        self._io.write(f"**************************************************\n")
        self._io.close()

    def initialize(self):
        """
        コントローラーを介した初期設定
        """
        print('############### Initializing ###############')
        start_time = time.perf_counter()
        self._controller.initialize()
        end_time = time.perf_counter()
        self._io.write(f"Initialize Time: {end_time-start_time} sec\n")


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
        brokerを展開し、コンテナが起動しているかを確認する
        """
        print('############### Deploying ###############')
        self._controller.remove_broker()
        start_time = time.perf_counter()
        self._controller.deploy_broker()
        self._check_container_running(self._controller.check_broker_running, self._check_count)
        end_time = time.perf_counter()
        self._io.write(f"Deploy Broker Time: {end_time-start_time} sec\n")
        
        time.sleep(10)  # クラスターの同期時間
        self._controller.create_topics()
        self._controller.describe_topics()

    def _collect_job(self):
        """
        publisher,subscriberが停止していることを確認してから結果を回収する
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
        start_time = time.perf_counter()
        self._controller.collect_container_results()
        self._controller.collect_logs()
        end_time = time.perf_counter()
        self._io.write(f"Collect Result Time: {end_time-start_time} sec\n")
        return schedule.CancelJob

    def _deploy_publisher(self):
        """
        """
        self._controller.deploy_publisher()
        try:
            self._check_container_running(self._controller.check_publisher_running, self._check_count)
        except RuntimeError as e:
            self._exec_info = e

    def _deploy_subscriber(self):
        """
        """
        self._controller.deploy_subscriber()
        try:
            self._check_container_running(self._controller.check_subscriber_running, self._check_count)
        except RuntimeError as e:
            self._exec_info = e

    def test_performance(self):
        """
        publisher,subscriberを展開し、パフォーマンステストを実行する
        """
        # トピックの状態を取得
        self._controller.describe_topics()

        # コンテナのIPを取得する
        self._controller.set_container_internal_ip()

        # actionsで実行するコンテナの初期設定
        self._controller.set_up_actions()

        # pulibhser,subscriberを展開
        print('############### Test perfomance ###############')
        start_time = time.perf_counter()
        thread_sub = threading.Thread(target=self._deploy_subscriber)
        thread_pub = threading.Thread(target=self._deploy_publisher)
        thread_sub.start()
        thread_pub.start()
        thread_sub.join()
        thread_pub.join()
        if self._exec_info is not None:
            raise self._exec_info
        end_time = time.perf_counter()
        self._io.write(f"Deploy Publisher And Subscriber Time: {end_time-start_time} sec\n")

        # 結果の回収,障害注入をスケジューラーに登録し待機する
        print(f"-------------- Execution Time: {controller.duration} sec --------------")
        self._scheduler.every(self._controller.duration).seconds.do(self._collect_job)
        self._controller.schedule_actions(self._scheduler)
        print(self._scheduler.get_jobs())
        while self._scheduler.get_jobs() != []:
            self._scheduler.run_pending()
            time.sleep(1)

        # publisher, subscriber, actionsコンテナを削除
        self._controller.remove_publisher()
        self._controller.remove_subscriber()
        self._controller.remove_actions()

    def clean(self):
        """
        展開した環境を削除する
        """
        print('############### Cleaning ###############')
        self._controller.remove_broker()
        self._controller.remove_publisher()
        self._controller.remove_subscriber()
        self._controller.remove_actions()
        start_time = time.perf_counter()
        self._controller.clean()
        end_time = time.perf_counter()
        self._io.write(f"Clean Time: {end_time-start_time} sec\n")

    def run(self):
        """
        環境構築 -> コンテナ展開 -> 結果収集 -> 環境削除の順に実行する
        """
        self.initialize()
        self.deploy()
        self.test_performance()
        self.clean()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', default='template.yml', help='Specify the template file (default: template.yml)')
    parser.add_argument('-m', '--mode', default='run', choices=['run', 'init', 'deploy', 'test', 'clean'],
                        help='Specify the execution mode (default: run the test)')
    parser.add_argument('--debug', action='store_true')
    args = parser.parse_args()

    # テンプレートファイルの読み込み
    with open(args.file, mode='r', encoding='utf-8') as f:
        template = yaml.safe_load(f)

    # ノード情報作成
    user = template['base']['user']
    if template['base']['keyfile'][0] == '$':
        keyfile = os.environ.get(template['base']['keyfile'][1:])
    else:
        keyfile = template['base']['keyfile']
    if template['base']['sudopass'][0] == '$':
        sudopass = os.environ.get(template['base']['sudopass'][1:])
    else:
        sudopass = template['base']['sudopass']
    if keyfile is None:
        raise RuntimeError('Please specify keyfile for ssh.')
    node_manager = NodeManager()
    for name, configs in template['base']['nodes'].items():
        node = Node(name, user, keyfile, sudopass, **configs)
        node_manager.append(node)

    # ノード情報とコンテナ情報を持つコントローラーを作成
    root_dir = 'exec_dir'
    executer = SwarmExecuter(home_dir=os.path.join(
        root_dir, 'executer'), remote_dir='/tmp/exec_dir', debug_mode=args.debug)
    if template["systems"]["type"] == 'kafka':
        controller = KafkaController(
            node_manager, executer, template['systems'], root_dir)

    # 発生障害の情報を作成する
    if 'actions' in template.keys():
        controller.create_actions(template['actions'])

    # コンテナの初期設定を行う
    controller.init_containers()

    # シミュレーションにコントローラーを設定し実行する
    ds = DmsSimulator(controller)
    ds.open()
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
    else:
        raise RuntimeError(f"Not implemented {args.mode}")
    ds.close()
