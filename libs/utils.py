import paramiko

class Node:
    def __init__(self, name, user, keyfile, address, hostname=None, is_manager=False):
        self.name = name
        self.user = user
        self.keyfile = keyfile
        self.address = address
        self.is_manager = is_manager
        self.hostname = hostname
        
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def ssh_exec(self, cmd):
        self._client.connect(self.address, username=self.user, key_filename=self.keyfile, timeout=5)
        stdin, stdout, stderr = self._client.exec_command(cmd)
        print(stdout.readlines())
        print(stderr.readlines())


    def ssh_exec_sudo(self):
        return

    def scp_put(self, local, remote):
        self._client.connect(self.address, username=self.user, key_filename=self.keyfile, timeout=5)
        with self._client.open_sftp() as sftp:
            sftp.put(local, remote)
        self._client.close()

    def scp_get(self, remote, local):
        self._client.connect(self.address, username=self.user, key_filename=self.keyfile)
        with self._client.open_sftp() as sftp:
            sftp.get(remote, local)
        self._client.close()


class NodeManager:
    def __init__(self):
        self._nodes = []

    def append(self, node):
        self._nodes.append(node)

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
