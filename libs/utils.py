import time
import paramiko


def connect(func):
    def exec(self, *args, **kwargs):
        self._client.connect(self.address, username=self.user,
                             key_filename=self.keyfile, timeout=5)
        result = func(self, *args, **kwargs)
        self._client.close()
        return result
    return exec


class Node:
    def __init__(self, name, user, keyfile, sudopass, address, is_manager=False):
        self.name = name
        self.user = user
        self.keyfile = keyfile
        self.sudopass = sudopass
        self.address = address
        self.is_manager = is_manager
        self.hostname = None

        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def init(self):
        # ホスト名設定
        stdin, stdout, stdrr = self.ssh_exec('hostname')
        self.hostname = stdout.readline().replace('\n', '')
        # print(self.hostname)

    @connect
    def ssh_exec(self, cmd):
        stdin, stdout, stderr = self._client.exec_command(cmd)
        stdout.channel.recv_exit_status()
        return (stdin, stdout, stderr)

    @connect
    def ssh_exec_sudo(self, cmd):
        stdin, stdout, stderr = self._client.exec_command(cmd, get_pty=True)
        cnt = 0
        while len(stdout.channel.in_buffer) == 0:
            time.sleep(0.1)
            cnt += 0.1
            if cnt == 5:
                print(f"Failur {cmd}")
                raise Exception
        stdin.channel.send(self.sudopass + "\n")
        stdin.flush()
        stdout.channel.recv_exit_status()
        return (stdin, stdout, stderr)

    @connect
    def scp_put(self, local, remote):
        with self._client.open_sftp() as sftp:
            sftp.put(local, remote)

    @connect
    def scp_get(self, remote, local):
        with self._client.open_sftp() as sftp:
            sftp.get(remote, local)

    def match_name(self, name):
        return self.name == name


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

    def get_nodes(self):
        return self._nodes

    def get_match_node(self, node_name):
        for node in self._nodes:
            if node.match_name(node_name):
                return node