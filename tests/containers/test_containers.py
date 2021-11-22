import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from libs.containers import KafkaContainer, ZookeeperContainer


if __name__ == '__main__':
    print('test')