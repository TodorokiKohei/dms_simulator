from abc import ABCMeta, abstractclassmethod


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


class Container(AbstractContainer):
    def __init__(self, name, node_name=None, ports=None, volumes=None, environment=None, networks=None,
                 command=None, **kwargs):
        self.image = None
        self.name = name
        self.node_name = node_name
        self.ports = ports
        self.volumes = volumes
        self.environemnt = environment
        self.networks = networks
        self.command = command

        self.home_dir = None
        self.node = None
        self.inner_ip = None

    def create_volume_dir(self):
        if self.volumes is None:
            return
        for vol in self.volumes:
            self.node.ssh_exec(f'mkdir -p {vol["source"]}')

    def delete_volume_dir(self):
        if self.volumes is None:
            return
        for vol in self.volumes:
            if vol['source'] == '/':
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
            'placement':
                {'constraints': [f'node.hostname=={self.node.hostname}']}
        }
        return {self.name: swarm}
