import os
from abc import ABCMeta, abstractclassmethod

from libs.base.executers import Executer
from libs.utils import NodeManager


class AbstrctController(metaclass=ABCMeta):
    @abstractclassmethod
    def initialize(self):
        """
        構築環境の初期化処理
        """
        pass

    @abstractclassmethod
    def init_container(self):
        """
        コンテナ起動可能にするための前処理
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
    def check_broker(self):
        """
        brokerの状態確認処理
        """
        pass

    @abstractclassmethod
    def check_publisher(self):
        """
        publisherの状態確認処理
        """
        pass

    @abstractclassmethod
    def check_subscriber(self):
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


class Controller(metaclass=ABCMeta):
    BROKER_SERVICE: str
    PUBLISHER_SERVICE: str
    SUBSCRIBER_SERVICE: str

    def __init__(self, node_manager: NodeManager, executer: Executer, systems: dict, root_dir: str):
        self._node_manager = node_manager
        self._executre = executer
        self._duration = systems['duration']
        self._home_dir = os.path.join(root_dir, 'controller')
        self._result_dir = os.path.join(root_dir, 'results')

        self._containers = []
        self._broker = []
        self._publisher = []
        self._subscriber = []

    @property
    def duration(self):
        sec = 0
        duration_str = self._duration
        for sep in ['h', 'm', 's']:
            if sep in self._duration:
                val, duration_str = duration_str.split(sep)
                if sep == 'h':
                    sec += int(val) * 60 * 60
                elif sep == 'm':
                    sec += int(val) * 60
                elif sep == 's':
                    sec += int(val)
        return sec

    def initialize(self):
        # コンテナのボリュームを作成
        for container in self._containers:
            container.create_volume_dir()
        # コンテナのPULL処理

        # executerの初期化処理
        self._executre.create_remote_dir(self._containers, self._node_manager)
        self._executre.create_cluster(self._containers, self._node_manager)
        self._executre.pull_container_image(
            self._containers, self._node_manager)

    def init_container(self):
        # コンテナを展開するノードを設定
        for container in self._containers:
            node = self._node_manager.get_match_node(container.node_name)
            if node is None:
                raise RuntimeError(
                    f'[{container.name}]: There are no matching nodes.')
            container.node = node

        # コンテナのログや設定ファイルを保存するディレクトリを設定
        for container in self._containers:
            container.home_dir = os.path.join(self._home_dir, container.name)
            os.makedirs(container.home_dir, exist_ok=True)

    def clean(self):
        # コンテナのボリュームを削除
        for container in self._containers:
            container.delete_volume_dir()
        # executerの掃除処理
        self._executre.delete_cluster(self._containers, self._node_manager)

    def deploy_broker(self):
        # brokerコンテナを展開
        print('------------Deploy broker containers------------')
        print(f"create {self.__class__.BROKER_SERVICE}")
        self._executre.up_containers(
            self._broker, self._node_manager, self.__class__.BROKER_SERVICE)

    def deploy_publisher(self):
        # publisherコンテナを展開
        print('------------Deploy publisher containers------------')
        print(f"create {self.__class__.PUBLISHER_SERVICE}")
        self._executre.up_containers(
            self._publisher, self._node_manager, self.__class__.PUBLISHER_SERVICE)

    def deploy_subscriber(self):
        # subscriberコンテナを展開
        print('------------Deploy subscriber containers------------')
        print(f"create {self.__class__.SUBSCRIBER_SERVICE}")
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
        print(f"remove {self.__class__.BROKER_SERVICE}")
        self._executre.down_containers(
            self._broker, self._node_manager, self.__class__.BROKER_SERVICE)

    def remove_publisher(self):
        print(f"remove {self.__class__.PUBLISHER_SERVICE}")
        self._executre.down_containers(
            self._broker, self._node_manager, self.__class__.PUBLISHER_SERVICE)

    def remove_subscriber(self):
        print(f"remove {self.__class__.SUBSCRIBER_SERVICE}")
        self._executre.down_containers(
            self._broker, self._node_manager, self.__class__.SUBSCRIBER_SERVICE)

    def collect_container_results(self):
        # コンテナが出力した結果を回収する
        for container in self._containers:
            container.collect_results()
