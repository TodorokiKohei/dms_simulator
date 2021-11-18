

class Node:
    def __init__(self, label, address, hostname=None, is_manager=False):
        self.label = label
        self.address = address
        self.is_manager = is_manager
        if hostname is None:
            self.hostname = label
        else:
            self.hostname = hostname

class NodeManager:
    def __init__(self, nodes):
        self._nodes = nodes

    def get_manager(self):
        for node in self._nodes:
            if node.is_manager:
                return node

    def get_workers(self):
        workers = []
        for node in self._nodes:
            if not node.is_manager:
                workers.append(node)
        return workers
