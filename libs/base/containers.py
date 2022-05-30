import glob
import os
from abc import ABCMeta, abstractclassmethod

from libs.utils import Node


class AbstractContainer(metaclass=ABCMeta):
    @abstractclassmethod
    def create_volume_dir(self):
        """
        コンテナを展開するノードでマウントするボリュームを作成する
        """
        pass

    @abstractclassmethod
    def delete_volume_dir(self):
        """
        コンテナを展開するノードでマウントするボリュームを削除する
        """
        pass

    @abstractclassmethod
    def to_swarm(self):
        """
        コンテナの属性からswarmで起動するための情報を作成する
        """
        pass

    @abstractclassmethod
    def pre_up_process(self):
        """
        コンテナ起動前の処理（設定ファイルのアップロード等）を記述する
        """
        pass

    @abstractclassmethod
    def collect_results(self):
        """
        コンテナのログや実行結果を収集する処理を記述する
        """
        pass

    @abstractclassmethod
    def record_container_info(self):
        """
        コンテナの情報を記録する
        """
        pass


class Container(AbstractContainer):
    def __init__(self, name, image=None, node_name=None, ports=None, volumes=None, environment=None, networks=None,
                 command=None, config_info=None):
        self.image = image
        self.name = name
        self.node_name = node_name
        self.ports = ports
        self.volumes = volumes
        self.environemnt = environment
        self.networks = networks
        self.command = command
        self.config_info = config_info
        self.home_dir = None
        self.node: Node = None
        
        self.internal_ip = None

    def create_volume_dir(self):
        """
        ssh接続を行いボリュームに指定されたディレクトリを作成する
        """
        if self.volumes is None:
            return
        for vol in self.volumes:
            if vol['source'] == '/var/run/docker.sock':
                continue
            self.node.ssh_exec(f'mkdir -p {vol["source"]}')

    def delete_volume_dir(self):
        """
        ssh接続を行いボリュームに指定されたディレクトリを削除する
        """
        if self.volumes is None:
            return
        for vol in self.volumes:
            if vol['source'] == '/':
                continue
            if vol['source'] == '/var/run/docker.sock':
                continue
            self.node.ssh_exec_sudo(f'sudo rm -rf {vol["source"]}')

    def to_swarm(self):
        swarm = {
            'image': self.image,
            'hostname': self.name,
        }
        if self.ports is not None:
            swarm['ports'] = self.ports
        if self.volumes is not None:
            swarm['volumes'] = self.volumes
        if self.environemnt is not None:
            swarm['environment'] = self.environemnt
        if self.networks is not None:
            swarm['networks'] = self.networks
        if self.command is not None:
            swarm['command'] = self.command
        swarm['deploy'] = {
            'mode': 'global',
            'endpoint_mode': 'dnsrr',
            'placement':
                {'constraints': [f'node.hostname=={self.node.hostname}']}
        }
        return {self.name: swarm}

    def record_container_info(self):
        if self.internal_ip is None:
            return
        file_path = os.path.join(
            self.get_result_path(), f"{self.name}_internal_ip")
        with open(file_path, mode="w") as f:
            f.write(self.internal_ip)

    def get_config_path(self):
        return os.path.join(self.home_dir, "configs")

    def get_result_path(self):
        return os.path.join(self.home_dir, "results")

    def transfer_configs(self):
        for conf_info in self.config_info:
            file_list = glob.glob(os.path.join(
                self.get_config_path(), conf_info['file']))
            for file in file_list:
                remote_filename = os.path.join(
                    conf_info['to'], os.path.basename(file))
                self.node.sftp_put(file, remote_filename)
