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
    def clean(self):
        """
        構築環境の掃除処理
        """
        pass

    @abstractclassmethod
    def deploy_broker(self):
        """
        ブローカーの展開処理
        """
        pass

    @abstractclassmethod
    def deploy_publisher(self):
        """
        パブリッシャーの展開処理
        """
        pass

    @abstractclassmethod
    def deploy_subscriber(self):
        """
        サブスクライバーの展開処理
        """
        pass

    @abstractclassmethod
    def check_broker(self):
        pass

    @abstractclassmethod
    def check_publisher():
        pass

    @abstractclassmethod
    def check_subscriber():
        pass


class Controller(metaclass=ABCMeta):
    def __init__(self, node_manager: NodeManager, executer: Executer, systems: dict, root_dir: str):
        self._node_manager = node_manager
        self._executre = executer
        self._home_dir = os.path.join(root_dir, 'controller')
        self._result_dir = os.path.join(root_dir, 'results')

        self._containers = []
        self._broker = []
        self._publisher = []
        self._subscriber = []

    def initialize(self):
        # コンテナのボリュームを作成
        for container in self._containers:
            container.create_volume_dir()
        # executerの初期化処理
        self._executre.create_remote_dir(self._containers, self._node_manager)
        self._executre.create_cluster(self._containers, self._node_manager)

    def clean(self):
        # コンテナのボリュームを削除
        for container in self._containers:
            container.delete_volume_dir()
        # executerの掃除処理
        self._executre.delete_cluster(self._containers, self._node_manager)

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
