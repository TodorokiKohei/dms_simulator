import os
from abc import ABCMeta, abstractclassmethod
from typing import List

from libs.base.containers import Container
from libs.utils import NodeManager


class AbstractExecuter(metaclass=ABCMeta):

    @abstractclassmethod
    def create_remote_dir(self, containers: List[Container], node_manager: NodeManager):
        """
        compose-fileなどを保存するためのリモートディレクトリを作成
        """
        pass

    @abstractclassmethod
    def create_cluster(self, containers: List[Container], node_manager: NodeManager):
        """
        Docker Swarmなどのクラスター構築処理
        """
        pass

    @abstractclassmethod
    def pull_container_image(self, containers: List[Container], node_manager: NodeManager):
        pass

    @abstractclassmethod
    def delete_cluster(self, containers: List[Container], node_manager: NodeManager):
        """
        Docker Swarmなどのクラスター削除処理
        """
        pass

    @abstractclassmethod
    def up_containers(self, containers: List[Container], node_manager: NodeManager, service: str):
        """
        クラスターにコンテナを展開する
        """
        pass

    @abstractclassmethod
    def down_containers(self, containers: List[Container], node_manager: NodeManager, service: str):
        """
        クラスターのコンテナを削除する
        """
        pass

    @abstractclassmethod
    def check_running(self, containers: List[Container], node_manager: NodeManager, service: str):
        """
        指定コンテナが起動しているかどうかを確認
        """
        pass

    @abstractclassmethod
    def collect_logs(self, containers: List[Container], node_manager: NodeManager, service: str):
        """
        コンテナが出力するログを回収する
        """
        pass

    @abstractclassmethod
    def pull_container_image(self, containers: List[Container], node_manager: NodeManager):
        """
        コンテナイメージを展開先ノードにpullする
        """
        pass

    @abstractclassmethod
    def get_container_internal_ip(self, containers: List[Container], node_manager: NodeManager):
        """
        コンテナの内部IPを取得する
        """
        pass


class Executer(AbstractExecuter):

    def __init__(self, home_dir, remote_dir):
        self._home_dir = home_dir
        self._remote_dir = remote_dir

        os.makedirs(self._home_dir, exist_ok=True)

    def pull_container_image(self, containers: List[Container], node_manager: NodeManager):
        for container in containers:
            images_and_tags, _ = container.node.ssh_exec("docker image ls --format {{.Repository}}:{{.Tag}}")
            images = [image_and_tag.split(":")[0] for image_and_tag in images_and_tags]
            if container.image in images_and_tags or container.image in images:
                continue
            container.node.ssh_exec(f"docker pull {container.image}")
