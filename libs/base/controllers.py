import copy
import os
import shutil
from threading import Thread
from typing import List
from abc import ABCMeta, abstractclassmethod
from datetime import datetime

import schedule
from libs import utils
from libs.base.containers import Container
from libs.base.executers import Executer
from libs.pumba import PumbaCrashContainer, PumbaNetemContainer, TcContainer
from libs.utils import NodeManager
from schedule import Scheduler


class AbstrctController(metaclass=ABCMeta):
    @abstractclassmethod
    def create_containers(self, systems:dict):
        """
        テンプレートファイルの「systems」からコンテナのインスタンスを作成
        """
        pass

    @abstractclassmethod
    def create_topic_info(self, systems:dict):
        """
        テンプレートファイルの「systems」からトピックの情報を作成
        """
        pass

    @abstractclassmethod
    def create_actions(self, actions):
        """
        テンプレートファイルを基に障害注入情報の作成
        """
        pass

    @abstractclassmethod
    def set_up_actions(self):
        """
        障害注入コンテナの初期設定
        """
        pass

    @abstractclassmethod
    def schedule_actions(self, scheduler):
        """
        障害注入のスケジュール設定
        """
        pass

    @abstractclassmethod
    def collect_logs(self):
        """
        コンテナのログを収集
        """
        pass

    @abstractclassmethod
    def remove_actions(self):
        """
        障害注入コンテナの削除
        """
        pass

    @abstractclassmethod
    def init_containers(self):
        """
        コンテナ情報の初期設定
        """
        pass

    @abstractclassmethod
    def initialize(self):
        """
        構築環境の初期化処理
        """
        pass

    @abstractclassmethod
    def clean(self):
        """
        構築環境の掃除処理
        """
        pass

    @abstractclassmethod
    def deploy_broker(self):
        """
        brokerの展開処理
        """
        pass

    @abstractclassmethod
    def deploy_publisher(self):
        """
        publisherの展開処理
        """
        pass

    @abstractclassmethod
    def deploy_subscriber(self):
        """
        subscriberの展開処理
        """
        pass

    @abstractclassmethod
    def check_broker_running(self):
        """
        brokerの状態確認処理
        """
        pass

    @abstractclassmethod
    def check_publisher_running(self):
        """
        publisherの状態確認処理
        """
        pass

    @abstractclassmethod
    def check_subscriber_running(self):
        """
        subscriberの状態確認処理
        """
        pass

    @abstractclassmethod
    def remove_broker(self):
        """
        brokerの削除処理
        """
        pass

    @abstractclassmethod
    def remove_publisher(self):
        """
        publisherの削除処理
        """
        pass

    @abstractclassmethod
    def remove_subscriber(self):
        """
        subscriberの削除処理
        """
        pass

    @abstractclassmethod
    def collect_container_results(self):
        """
        全てのコンテナの結果を収集する処理
        """
        pass

    @abstractclassmethod
    def set_container_internal_ip(self):
        """
        全てのコンテナの内部IPを取得し、設定する
        """
        pass

    @abstractclassmethod
    def create_topics(self):
        pass

    @abstractclassmethod
    def describe_topics(self):
        pass


class Controller(AbstrctController):
    BROKER: str
    BROKER_SERVICE: str
    PUBLISHER_SERVICE: str
    SUBSCRIBER_SERVICE: str

    def __init__(self, node_manager: NodeManager, executer: Executer, systems: dict, root_dir: str):
        self._node_manager = node_manager
        self._executre = executer
        self._duration = systems['duration']
        self._home_dir = os.path.join(root_dir, 'controller', self.__class__.BROKER)
        self._topic_dir = os.path.join(self._home_dir, 'topic_configs')

        self._containers: List[Container] = []
        self._broker: List[Container] = []
        self._publisher: List[Container] = []
        self._subscriber: List[Container] = []
        self._actions: List[Container] = []
        self._tc_containers: List[Container] = []

    @property
    def duration(self):
        return utils.change_time_to_sec(self._duration)

    def _set_container_node(self, container: Container):
        """
        コンテナの展開先のノード名と一致するノードクラスを設定する
        """
        node = self._node_manager.get_match_node(container.node_name)
        if node is None:
            raise RuntimeError(
                f'[{container.name}]: There are no matching nodes.')
        container.node = node

    def _set_and_create_container_home_dir(self, container: Container):
        """
        コンテナの結果を保存するディレクトリパスを設定・作成する
        """
        container.home_dir = os.path.join(self._home_dir, container.name)
        os.makedirs(container.get_result_path(), exist_ok=True)

    def _search_container(self, container_name: str):
        """
        コンテナ名と一致するコンテナを返す
        """
        for container in self._containers:
            if container_name == container.name:
                return container
        return None

    def create_actions(self, actions: dict):
        for action_name, action_configs in actions.items():
            # ターゲットコンテナ毎に作成
            target_containers = action_configs.pop('target_containers')
            for i, tgt in enumerate(target_containers):
                action_name = f'{action_name}-{str(i)}'
                configs = copy.deepcopy(action_configs)
                if action_configs['mode'] in ['delay', 'loss', 'rate']:
                    self._actions.append(PumbaNetemContainer(action_name, configs, tgt))
                elif action_configs['mode'] in ['kill', 'pause', 'stop', 'rm']:
                    self._actions.append(PumbaCrashContainer(action_name, configs, tgt))
                else:
                    raise NotImplementedError(
                        f'{action_configs["mode"]} is not implemented')

    def set_up_actions(self):
        for action_container in self._actions:
            # ターゲットのコンテナと同じノード、同じネットワークになるよう設定
            target_container = self._search_container(
                action_container.target_container)
            action_container.node_name = target_container.node_name
            action_container.networks = target_container.networks
            self._set_container_node(action_container)

            if action_container.is_netem():
                tc_container = TcContainer()
                self._tc_containers.append(tc_container)
                tc_container.node_name = target_container.node_name
                self._set_container_node(tc_container)

            # ログを保存するローカルのディレクトリを作成
            self._set_and_create_container_home_dir(action_container)

            # コマンド作成
            action_container.create_commnad(self._containers)

        # 起動を高速にするため、事前にイメージのPull処理
        self._executre.pull_container_image(self._actions, self._node_manager)
        self._executre.pull_container_image(self._tc_containers, self._node_manager)

    def _deploy_action(self, action_container):
        """
        指定された障害注入コンテナを起動する
        """
        # action_container.create_commnad(self._containers)
        self._executre.up_containers(
            [action_container], self._node_manager, action_container.name)
        return schedule.CancelJob

    def schedule_actions(self, scheduler: Scheduler):
        # 障害注入コンテナをスケジューラーに登録
        for action_container in self._actions:
            scheduler.every(action_container.start).seconds.do(
                self._deploy_action, action_container=action_container)

    def collect_logs(self):
        # コンテナのログを回収する
        self._executre.collect_logs(self._publisher, self._node_manager, self.PUBLISHER_SERVICE)
        self._executre.collect_logs(self._subscriber, self._node_manager, self.SUBSCRIBER_SERVICE)
        for action_container in self._actions:
            self._executre.collect_logs(
                [action_container], self._node_manager, action_container.name)

    def remove_actions(self):
        # 障害注入コンテナの削除
        for action_container in self._actions:
            self._executre.down_containers(
                [action_container], self._node_manager, action_container.name)

    def init_containers(self):
        # コンテナを展開するノードを設定
        for container in self._containers:
            self._set_container_node(container)

        # コンテナのログや設定ファイルを保存するローカルのディレクトリパスを設定・作成する
        for container in self._containers:
            self._set_and_create_container_home_dir(container)

    def initialize(self):
        # executerの初期化処理
        self._executre.create_remote_dir(self._containers, self._node_manager)
        self._executre.create_cluster(self._containers, self._node_manager)

    def clean(self):
        # executerの掃除処理
        self._executre.delete_cluster(self._containers, self._node_manager)

    def deploy_broker(self):
        # brokerコンテナを展開
        print('------------Deploy broker containers------------')
        print(f"create {self.__class__.BROKER_SERVICE}")
        # for container in self._broker:
        #     container.create_volume_dir()
        # self._executre.pull_container_image(self._broker, self._node_manager)
        # self._executre.up_containers(
            # self._broker, self._node_manager, self.__class__.BROKER_SERVICE)

        thread_list = []
        for container in self._broker:
            th = Thread(target=self._executre.pre_deploy_process, args=(container, self._node_manager,))
            th.start()
            thread_list.append(th)
        for th in thread_list:
            th.join()

        self._executre.up_containers(
            self._broker, self._node_manager, self.__class__.BROKER_SERVICE)

    def deploy_publisher(self):
        # publisherコンテナを展開
        print('------------Deploy publisher containers------------')
        print(f"create {self.__class__.PUBLISHER_SERVICE}")
        thread_list = []
        for container in self._publisher:
            th = Thread(target=self._executre.pre_deploy_process, args=(container, self._node_manager,))
            th.start()
            thread_list.append(th)
        for th in thread_list:
            th.join()
        self._executre.up_containers(
            self._publisher, self._node_manager, self.__class__.PUBLISHER_SERVICE)

    def deploy_subscriber(self):
        # subscriberコンテナを展開
        print('------------Deploy subscriber containers------------')
        print(f"create {self.__class__.SUBSCRIBER_SERVICE}")
        thread_list = []
        for container in self._subscriber:
            th = Thread(target=self._executre.pre_deploy_process, args=(container, self._node_manager,))
            th.start()
            thread_list.append(th)
        for th in thread_list:
            th.join()
        self._executre.up_containers(
            self._subscriber, self._node_manager, self.__class__.SUBSCRIBER_SERVICE)

    def check_broker_running(self):
        # brokerの起動状態を確認する
        return self._executre.check_running(self._broker, self._node_manager, self.BROKER_SERVICE)

    def check_publisher_running(self):
        # publisherの起動状態を確認する
        return self._executre.check_running(self._publisher, self._node_manager, self.PUBLISHER_SERVICE)

    def check_subscriber_running(self):
        # subscriberの起動状態を確認する
        return self._executre.check_running(self._subscriber, self._node_manager, self.SUBSCRIBER_SERVICE)

    def remove_broker(self):
        # brokerの削除を行う
        print(f"remove {self.__class__.BROKER_SERVICE}")
        self._executre.down_containers(
            self._broker, self._node_manager, self.__class__.BROKER_SERVICE)

        thread_list = []
        for container in self._broker:
            th = Thread(target=container.delete_volume_dir)
            th.start()
            thread_list.append(th)
        for th in thread_list:
            th.join()

    def remove_publisher(self):
        # publisherの削除を行う
        print(f"remove {self.__class__.PUBLISHER_SERVICE}")
        self._executre.down_containers(
            self._publisher, self._node_manager, self.__class__.PUBLISHER_SERVICE)
        for container in self._publisher:
            container.delete_volume_dir()

    def remove_subscriber(self):
        # subscriberの削除を行う
        print(f"remove {self.__class__.SUBSCRIBER_SERVICE}")
        self._executre.down_containers(
            self._subscriber, self._node_manager, self.__class__.SUBSCRIBER_SERVICE)
        for container in self._subscriber:
            container.delete_volume_dir()

    def collect_container_results(self):
        # コンテナが出力したファイルを回収する
        for container in self._containers:
            container.collect_results()
            container.record_container_info()

    def set_container_internal_ip(self):
        # コンテナの内部IPを取得・設定する  
        internal_ip_info_list = self._executre.get_container_internal_ip(self._containers, self._node_manager)
        for internal_ip_info in internal_ip_info_list:
            for container in self._containers:
                if internal_ip_info['name'].endswith(container.name):
                    container.internal_ip = internal_ip_info['ip']
                    break

    def create_configs_dir(self):
        for container in self._containers:
            os.makedirs(container.get_config_path(), exist_ok=True)
        p = os.path.join(self._topic_dir)
        os.makedirs(p, exist_ok=True)

    def remove_results(self):
        # 実行結果の削除を行う
        for container in self._containers:
            if os.path.exists(container.get_result_path()):
                shutil.rmtree(container.get_result_path())

    def record_test_time(self, start_time:datetime, end_time:datetime):
        with open(os.path.join(self._home_dir, "test_record_time"), "a") as f:
            f.write("****************************************************************\n")
            f.write(self.__class__.__name__ + "\n")
            f.write("開始時刻: " + start_time.strftime("%Y/%m/%d %H:%M:%S") + "\n")
            f.write("終了時刻: " + end_time.strftime("%Y/%m/%d %H:%M:%S") + "\n")
            f.write("****************************************************************\n\n")

