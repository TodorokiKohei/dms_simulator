

import argparse
import math
import os
from glob import glob

import numpy as np
import matplotlib.pyplot as plt


def collect_pub_throughput(root_dir):
    pub_file_list = glob(f"{root_dir}/**/*publisher*throuput*")
    pub_file_list.sort()
    result = {}
    for pub_file_path in pub_file_list:
        dir_name = os.path.basename(os.path.dirname(pub_file_path))
        dir_info = dir_name.split('_')
        broker = dir_info[2]
        message_size = dir_info[5]
        if (broker == 'kafka'):
            acks = dir_info[7]
            broker = f'{broker}-ack-{acks}'
        pub_throughput = np.loadtxt(
            pub_file_path, delimiter=',', skiprows=1, dtype=np.int64)
        if broker not in result.keys():
            result[broker] = {}
        result[broker][message_size] = pub_throughput
    return result


def plot_bar_for_each(root_dir, throughput_map, mode):
    fig, ax = plt.subplots()
    for broker, msg_size_map in throughput_map.items():
        x = np.arange(len(msg_size_map))
        mean_msg = []
        std_msg = []
        mean_byte = []
        std_byte = []
        x_ticklabels = []
        for sz, throughput in msg_size_map.items():
            mean_msg.append(throughput[2:, 1].mean())
            std_msg.append(throughput[2:, 1].std())
            mean_byte.append((throughput[2:, 2]/1000/1000).mean())
            std_byte.append((throughput[2:, 2]/1000/1000).std())
            x_ticklabels.append(sz)
        plt.bar(x, np.array(mean_msg), yerr=np.array(std_msg), capsize=5)
        ax.set_title(f"{broker}")
        ax.set_ylabel("Throughput[msg/s]")
        ax.set_xlabel("Message Size")
        ax.set_xticks(x)
        ax.set_xticklabels(x_ticklabels)
        fig.savefig(os.path.join(root_dir, f'{mode}_{broker}_msg.png'))
        ax.cla()

        plt.bar(x, np.array(mean_byte), yerr=np.array(std_byte), capsize=5)
        ax.set_title(f"{broker}")
        ax.set_ylabel("Throughput[MB/s]")
        ax.set_xlabel("Message Size")
        ax.set_xticks(x)
        ax.set_xticklabels(x_ticklabels)
        fig.savefig(os.path.join(root_dir, f'{mode}_{broker}_byte.png'))
        ax.cla()


def plot_bar_from_key(root_dir, throughput_map, mode, keys):
    fig_msg, ax_msg = plt.subplots()
    fig_byte, ax_byte = plt.subplots()
    match_keys = [key for key in throughput_map.keys() if key in keys]
    width = 0.4
    x_start = 0
    x = []
    x_ticks = []
    x_ticklabels = []
    labels = []
    mean_msg = []
    std_msg = []
    mean_byte = []
    std_byte = []
    is_first = True
    for broker in match_keys:
        msg_size_map = throughput_map[broker]
        # 棒グラフのサイズ分だけずらしたxの値を作成
        x_range = [x_start + i*width for i in range(len(msg_size_map))]
        x.extend(x_range)
        x_start += math.ceil(width * len(msg_size_map)) + 1
        x_ticks.append((x_range[-1] + x_range[0])/2)
        x_ticklabels.append(broker)
        for sz, throughput in msg_size_map.items():
            mean_msg.append(throughput[2:, 1].mean())
            std_msg.append(throughput[2:, 1].std())
            mean_byte.append((throughput[2:, 2]/1000/1000).mean())
            std_byte.append((throughput[2:, 2]/1000/1000).std())
            if (is_first):
                labels.append(sz)
        is_first = False

    sz_num = len(labels)
    for i, label in enumerate(labels):
        ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
                yerr=np.array(std_msg[i::sz_num]), width=width, capsize=2, label=label)
        ax_byte.bar(x[i::sz_num], np.array(mean_byte[i::sz_num]),
                yerr=np.array(std_byte[i::sz_num]), width=width, capsize=2, label=label)

    
    ax_msg.set_title("_".join(match_keys))
    ax_msg.set_ylabel("Throughput[msg/s]")
    ax_msg.set_xticks(x_ticks)
    ax_msg.set_xticklabels(x_ticklabels)
    ax_msg.legend()
    fig_msg.savefig(os.path.join(root_dir, f'{mode}_{"_".join(match_keys)}_msg.png'))

    ax_byte.set_title("_".join(match_keys))
    ax_byte.set_ylabel("Throughput[MB/s]")
    ax_byte.set_xticks(x_ticks)
    ax_byte.set_xticklabels(x_ticklabels)
    ax_byte.legend()
    fig_byte.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_byte.png'))


def plot_pub_throughput(root_dir):
    pub_throughput_map = collect_pub_throughput(root_dir)

    # 各結果
    plot_bar_for_each(root_dir, pub_throughput_map, "publish")

    # まとめ結果
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-ack-0", "kafka-ack-1"])
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-ack-0", "kafka-ack-1", "jetstream", "nats"])

    # kafka
    # keys = [key for key in pub_throughput_map.keys() if 'kafka' in key]
    # for key in keys:
    #     res = pub_throughput_map[key]

    # jetstream

    # nats


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', required=True)
    args = parser.parse_args()

    root_dir = args.dir
    plot_pub_throughput(root_dir)
