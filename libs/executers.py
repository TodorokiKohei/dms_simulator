import os
from abc import ABCMeta, abstractclassmethod

import paramiko

from libs.utils import Node


class Executer(metaclass=ABCMeta):
    def __init__(self, base: dict):
        self.user = base['User']
        if base['Password'][0] == '$':
            self.password = os.environ.get(base['Password'][1:])
        else:
            self.password = base['Password']
        if base['Keyfile'][0] == '$':
            self.keyfile = os.environ.get(base['Keyfile'][1:])
        else:
            self.keyfile = base['Keyfile']

        self.nodes = []
        for name, node_config in base['Nodes'].items():
            self.nodes.append(Node(name, **node_config))


    @abstractclassmethod
    def init(self):
        pass


    @abstractclassmethod
    def clean(self):
        pass

class SwarmExecuter(Executer):
    def init(self):
        # swarmの初期化
        # worker追加
        pass


    def clean(self):
        # ノードの除外
        # swarmの削除
        pass
