
import csv
import math
import os
import json
import time
from datetime import datetime, timedelta, timezone
from glob import glob
import re

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = "Meiryo"
# plt.rcParams["font.size"] = 13


def get_broker_info_from_file(file_path):
    dir_name = os.path.basename(os.path.dirname(file_path))
    dir_info = dir_name.split('_')
    broker = dir_info[2]
    message_size = dir_info[5]
    if (len(dir_info) > 6):
        broker = f'{broker}-{"-".join(dir_info[6:])}'
    return (broker, message_size)


def collect_throughput(root_dir, pattern):
    file_list = glob(os.path.join(root_dir, pattern))
    file_list.sort()
    result = {}
    for file_path in file_list:
        broker, message_size = get_broker_info_from_file(file_path)
        throughput = np.loadtxt(
            file_path, delimiter=',', skiprows=1, dtype=np.int64)
        if broker not in result.keys():
            result[broker] = {}
        result[broker][message_size] = throughput
    return result


def collect_message_info(root_dir, pattern):
    file_list = glob(os.path.join(root_dir, pattern))
    file_list.sort()
    message_df_list = {}
    for file_path in file_list:
        broker, message_size = get_broker_info_from_file(file_path)
        message_df = pd.read_csv(file_path)
        if broker not in message_df_list.keys():
            message_df_list[broker] = {}
        message_df_list[broker][message_size] = message_df
    return message_df_list

    # fig, ax = plt.subplots()
    # ax.plot(message_df.loc[:,'seqNum'],
    #  message_df.loc[:,'receivedTime']-message_df.loc[:,'sentTime'])
    # fig.savefig(os.path.join(root_dir, f'{broker}_{message_size}_latency.png'))
    # ax.cla()


def plot_latency_for_each(root_dir, df_list):
    fig, ax = plt.subplots()
    for broker, msg_size_df_list in df_list.items():
        for msg_size, df in msg_size_df_list.items():
            x = df['sentTime'] - df.loc[0, 'sentTime']
            y = df['receivedTime'] - df['sentTime']
            plt.plot(x, y)
            ax.set_title(f"{broker} {msg_size}")
            ax.set_ylabel("Latency[ms]")
            ax.set_xlabel("Elapsed Time[sec]")
            fig.savefig(os.path.join(root_dir, f'latency_each_{broker}_{msg_size}.png'))
            ax.cla()


def plot_bar_from_key_latency(root_dir, latency_map, mode, match_keys, **kwargs):
    if 'figsize' in kwargs.keys():
        fig, ax = plt.subplots(figsize=kwargs['figsize'])
    else:
        fig, ax = plt.subplots()
    if match_keys == []:
        match_keys = latency_map.keys()
    width = 0.4
    start_i = 2
    x_start = 0
    x = []
    x_ticks = []
    x_ticklabels = []
    labels = []
    mean_latency = []
    std_latency = []
    is_first = True
    for key in match_keys:
        msg_size_map = latency_map[key]
        # 棒グラフのサイズ分だけずらしたxの値を作成
        x_range = [x_start + i*width for i in range(len(msg_size_map))]
        x.extend(x_range)
        x_start += math.ceil(width * len(msg_size_map)) + 1
        x_ticks.append((x_range[-1] + x_range[0])/2)
        x_ticklabels.append(key)
        for sz, latency in msg_size_map.items():
            mean_latency.append(latency[start_i:].mean())
            std_latency.append(latency[start_i:].std())
            if (is_first):
                labels.append(sz)
        is_first = False

    sz_num = len(labels)
    for i, label in enumerate(labels):
        # ax.bar(x[i::sz_num], np.array(mean_latency[i::sz_num]),
        #        yerr=np.array(std_latency[i::sz_num]), width=width, capsize=2, label=label)
        ax.bar(x[i::sz_num], np.array(mean_latency[i::sz_num]),
               width=width, capsize=2, label=label)

    with open(os.path.join(root_dir, f'latency_avg.csv'), mode='w') as f:
        writer = csv.writer(f)
        header = ['size']
        header.extend(match_keys)
        writer.writerow(header)
        for i, label in enumerate(labels):
            data = [label]
            data.extend(mean_latency[i::sz_num])
            writer.writerow(data)

    if ('title' in kwargs.keys()):
        ax.set_title(kwargs['title'])
    else:
        ax.set_title("_".join(match_keys))

    if ('xlabel' in kwargs.keys()):
        ax.set_xlabel(kwargs['xlabel'])

    if ('ylabel' in kwargs.keys()):
        ax.set_ylabel(kwargs['ylabel'])
    else:
        ax.set_ylabel("latency[ms]")

    ax.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax.set_xticklabels(x_ticklabels)

    if ('yscale' in kwargs.keys()):
        ax.set_yscale(kwargs['yscale'])

    if ('ylim' in kwargs.keys()):
        ax.set_ylim(kwargs['ylim'])

    ax.legend()
    fig.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_msg.png'), bbox_inches='tight')


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

def plot_bar_from_key(root_dir, throughput_map, mode, match_keys, res_lower_limit, **kwargs):
    if 'figsize' in kwargs.keys():
        fig_msg, ax_msg = plt.subplots(figsize=kwargs['figsize'])
        fig_byte, ax_byte = plt.subplots(figsize=kwargs['figsize'])
    else:
        fig_msg, ax_msg = plt.subplots()
        fig_byte, ax_byte = plt.subplots()
    if match_keys == []:
        match_keys = throughput_map.keys()
    width = 0.4
    start_i = 4
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
            mean_msg.append(throughput[start_i:-1, 1].mean())
            std_msg.append(throughput[start_i:-1, 1].std())
            mean_byte.append((throughput[start_i:-1, 2]/1000/1000).mean())
            std_byte.append((throughput[start_i:-1, 2]/1000/1000).std())
            if (is_first):
                labels.append(sz)
        is_first = False

    sz_num = len(labels)
    for i, label in enumerate(labels):
        # ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
        #            yerr=np.array(std_msg[i::sz_num]), width=width, capsize=2, label=label)
        # ax_byte.bar(x[i::sz_num], np.array(mean_byte[i::sz_num]),
        #             yerr=np.array(std_byte[i::sz_num]), width=width, capsize=2, label=label)
        ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
                   width=width, capsize=2, label=label)
        # ax_msg.bar(x[i::sz_num], np.array(mean_msg[i::sz_num]),
        #            yerr=np.array([min_msg[i::sz_num], max_msg[i::sz_num]]), width=width, capsize=2, label=label)
        ax_byte.bar(x[i::sz_num], np.array(mean_byte[i::sz_num]),
                    width=width, capsize=2, label=label)

    with open(os.path.join(root_dir, f'{mode}_avg_msg.csv'), mode='w') as f:
        writer = csv.writer(f)
        header = ['size']
        header.extend(match_keys)
        writer.writerow(header)
        for i, label in enumerate(labels):
            data = [label]
            data.extend(mean_msg[i::sz_num])
            writer.writerow(data)


    with open(os.path.join(root_dir, f'{mode}_avg_byte.csv'), mode='w') as f:
        writer = csv.writer(f)
        header = ['size']
        header.extend(match_keys)
        writer.writerow(header)
        for i, label in enumerate(labels):
            data = [label]
            data.extend(mean_byte[i::sz_num])
            writer.writerow(data)

    if ('title' in kwargs.keys()):
        ax_msg.set_title(kwargs['title'])
    else:
        ax_msg.set_title("_".join(match_keys))

    if ('xlabel' in kwargs.keys()):
        ax_msg.set_xlabel(kwargs['xlabel'])

    if ('ylabel' in kwargs.keys()):
        ax_msg.set_ylabel(kwargs['ylabel'])
    else:
        ax_msg.set_ylabel("Throughput[msg/s]")

    ax_msg.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax_msg.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax_msg.set_xticklabels(x_ticklabels)

    if ('msg_yscale' in kwargs.keys()):
        ax_msg.set_yscale(kwargs['msg_yscale'])
    
    if ('msg_ylim' in kwargs.keys()):
        ax_msg.set_ylim(kwargs['msg_ylim'])

    ax_msg.legend()
    fig_msg.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_msg.png'), bbox_inches='tight')

    if ('title' in kwargs.keys()):
        ax_byte.set_title(kwargs['title'])
    else:
        ax_byte.set_title("_".join(match_keys))

    if ('xlabel' in kwargs.keys()):
        ax_byte.set_xlabel(kwargs['xlabel'])

    if ('ylabel' in kwargs.keys()):
        ax_byte.set_ylabel(kwargs['ylabel'])
    else:
        ax_byte.set_ylabel("Throughput[MB/s]")

    ax_byte.set_xticks(x_ticks)
    if ('xtickslabels' in kwargs.keys()):
        ax_byte.set_xticklabels(kwargs['xtickslabels'])
    else:
        ax_byte.set_xticklabels(x_ticklabels)

    if ('byte_yscale' in kwargs.keys()):
        ax_byte.set_yscale(kwargs['byte_yscale'])

    if ('byte_ylim' in kwargs.keys()):
        ax_byte.set_ylim(kwargs['byte_ylim'])

    ax_byte.legend()
    fig_byte.savefig(os.path.join(
        root_dir, f'{mode}_{"_".join(match_keys)}_byte.png'), bbox_inches='tight')


####################################################################

def plot_single(res_lower_limit):
    """
    1台構成でのプロット
    """
    root_dir = 'results/2022_0610_2359_100b_64kb'

    # Publishserのスループット
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throuput*")
    pub_throughput_map.update(collect_throughput(
        root_dir, "**/*publisher*throughput*"))
    # plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", ['kafka-acks-0', 'kafka-acks-1', 'nats', 'jetstream'], res_lower_limit,
                      title="", msg_yscale='log')
                    #    , msg_yscale='log', byte_yscale='log')

    # Subscriberのスループット
    sub_throughput_map = collect_throughput(
        root_dir, "**/*subscriber*throuput*")
    sub_throughput_map.update(collect_throughput(
        root_dir, "**/*subscriber*throughput*"))
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ['kafka-acks-0', 'kafka-acks-1', 'nats', 'jetstream'], res_lower_limit,
                      title="1台構成でのSubscriberのスループット", msg_yscale='log')
                    #    , msg_yscale='log', byte_yscale='log')

    # # PublisherとSubscriberのスループット比較
    # for broker, pub_throughput in pub_throughput_map.items():
    #     throughput_map = {}
    #     throughput_map['pub_' + broker] = pub_throughput
    #     throughput_map['sub_' + broker] = sub_throughput_map[broker]
    #     plot_bar_from_key(root_dir, throughput_map, f'pub-sub-{broker}',
    #                       ['pub_'+broker, 'sub_'+broker], res_lower_limit)

    # Broker毎のレイテンシー
    broker_list = ['kafka', 'jetstream', 'nats']
    latency_map = {}
    for broker in broker_list:
        if broker == 'kafka':
            df_list = collect_message_info(
                root_dir, f"**/*{broker}-subscriber-?-?.csv")
        elif broker == 'jetstream':
            df_list = collect_message_info(
                root_dir, f"**/*{broker}-subscriber-pull-?.csv")
        else:
            df_list = collect_message_info(
                root_dir, f"**/*{broker}-subscriber-?.csv")
        for broker in df_list:
            latency = {}
            for msg_size, df in df_list[broker].items():
                latency[msg_size] = (
                    df.loc[:, 'receivedTime'] - df.loc[:, 'sentTime']).values
            latency_map[broker] = latency
        plot_latency_for_each(root_dir, df_list)
    plot_bar_from_key_latency(root_dir, latency_map, "latency", ['kafka-acks-0', 'kafka-acks-1', 'nats', 'jetstream'],
                              title="", yscale='log')


####################################################################

def plot_single_kafka(res_lower_limit):
    """
    Kafkaの1台構成でのプロット
    """
    root_dir = 'results/2022_0604_kafka_buff_100b_16kb'
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throuput*")
    plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    plot_bar_from_key(root_dir, pub_throughput_map, "publish", [
                      "kafka-acks-0", "kafka-acks-1", "kafka-acks-0-buff-32k-linger-10", "kafka-acks-1-buff-32k-linger-10"],
                      res_lower_limit,
                      title="Kafka when changing parameters",
                      xtickslabels=["ack=0", "ack=1", "ack=0\nch params", "ack=1\nch params"])

    # sub_throughput_map = collect_throughput(root_dir, "**/*subscriber*throuput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", [
    #                   "kafka-acks-0", "kafka-acks-1", "kafka-acks-0-buff-32k-linger-10", "kafka-acks-1-buff-32k-linger-10"],
    #                   title="Kafka when changing parameters",
    #                   xtickslabels=["ack=0", "ack=1", "ack=0\nch params", "ack=1\nch params"])


####################################################################

def plot_cluster(res_lower_limit):
    """
    クラスター構成でのプロット
    """
    # root_dir = 'results/2022_0609_2256_cluster_100b_8kb'
    root_dir = 'results/2022_0611_0324_cluster_100b_64kb'

    # pub_throughput_map = collect_throughput(root_dir, "**/*publisher*throuput*")
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throughput*")
    # plot_bar_for_each(root_dir, pub_throughput_map, "publish")
    # plot_bar_from_key(root_dir, pub_throughput_map, "publish", ["kafka-acks-0-cl-3", "kafka-acks-1-cl-3", "jetstream-cl-3"])

    sub_throughput_map = collect_throughput(
        root_dir, "**/*subscriber*throughput*")
    # plot_bar_for_each(root_dir, sub_throughput_map, "subscribe")
    # plot_bar_from_key(root_dir, sub_throughput_map, "subscribe", ["kafka-acks-0-cl-3", "kafka-acks-1-cl-3", "jetstream-cl-3"])

    throughput_all_map = {}
    xticks_labels = []
    for broker, _ in pub_throughput_map.items():
        broker_info = broker.split('-')
        if len(broker_info) != 1 and broker_info[-2] == 'cl':
            continue
        throughput_map = {}
        throughput_map['pub-'+broker] = pub_throughput_map[broker]
        throughput_map['pub-'+broker +
                       '-cl-3'] = pub_throughput_map[broker+'-cl-3']
        # throughput_map['sub-'+broker] = sub_throughput_map[broker]
        # throughput_map['sub-'+broker +
        #                '-cl-3'] = pub_throughput_map[broker+'-cl-3']

        
        if broker == 'kafka-acks-1':
            xticks_labels.extend(['Kafka(ACK=1)'+'\nスタンドアロン', 'Kafka(ACK=1)'+'\nクラスター'])
        elif broker == 'kafka-acks-all':
            xticks_labels.extend(['Kafka(ACK=all)'+'\nスタンドアロン', 'Kafka(ACK=all)'+'\nクラスター'])
        elif broker == 'jetstream':
            xticks_labels.extend(['JetStream'+'\nスタンドアロン', 'JetStream'+'\nクラスター'])
        # xticks_labels.extend([broker+'\nsingle', broker+'\nreplication'])
        throughput_all_map['pub-'+broker] = pub_throughput_map[broker]
        throughput_all_map['pub-'+broker+'-cl-3'] = pub_throughput_map[broker+'-cl-3']
        # if 'jetstream' in broker_info:
        #     dummy = []
        #     for _ in range(10):
        #         dummy.append([0, 1, 0])
        #     dummy = np.array(dummy)
        #     throughput_all_map['pub-'+broker]['16kb'] = dummy
        #     throughput_all_map['pub-'+broker]['32kb'] = dummy
        #     throughput_all_map['pub-'+broker]['64kb'] = dummy
        #     throughput_all_map['pub-'+broker+'-cl-3']['16kb'] = dummy
        #     throughput_all_map['pub-'+broker+'-cl-3']['32kb'] = dummy
        #     throughput_all_map['pub-'+broker+'-cl-3']['64kb'] = dummy
        
        plot_bar_from_key(root_dir, throughput_map, 'pub-sub', throughput_map.keys(), res_lower_limit,
                          title='', 
                        #   xtickslabels=['pub-single', 'pub-replication', 'sub-single', 'sub-replication'], 
                          xtickslabels=['pub-single', 'pub-replication'], msg_yscale='log')
                        #   msg_yscale='log', 
                        #   byte_yscale='log')

    plot_bar_from_key(root_dir, throughput_all_map, 'pub-sub', throughput_all_map.keys(), res_lower_limit,
                          title='', 
                          xtickslabels=xticks_labels,
                          msg_yscale='log',
                          msg_ylim=[10, 4*10**5],
                          figsize=(9, 4.8))

    latency_all_map = {}
    match_keys = []
    xticks_labels = []
    for broker, _ in pub_throughput_map.items():
        broker_info = broker.split('-')
        if len(broker_info) != 1 and broker_info[-2] == 'cl':
            continue

        latency_map = {}
        if broker_info[0] == 'kafka' and broker_info[2] == '1':
            xticks_labels.extend(["kafka-acks-1\nsingle", "kafka-acks-1\nreplication"])
            match_keys.extend(['kafka-acks-1', 'kafka-acks-1-cl-3'])
            df_dict = collect_message_info(
                root_dir, "**acks_1/*kafka-subscriber-?-?.csv")
            df_dict.update(collect_message_info(
                root_dir, "**acks_1_cl_3/*kafka-subscriber-?-?.csv"))
        elif broker_info[0] == 'kafka' and broker_info[2] == 'all':
            xticks_labels.extend(["kafka-acks-all\nsingle", "kafka-acks-all\nreplication"])
            match_keys.extend(['kafka-acks-all', 'kafka-acks-all-cl-3'])
            df_dict = collect_message_info(
                root_dir, "**acks_all/*kafka-subscriber-?-?.csv")
            df_dict.update(collect_message_info(
                root_dir, "**acks_all_cl_3/*kafka-subscriber-?-?.csv"))
        elif broker_info[0] == 'jetstream':
            xticks_labels.extend(["jetstream\nsingle", "jetstream\nreplication"])
            match_keys.extend(['jetstream', 'jetstream-cl-3'])
            df_dict = collect_message_info(
                root_dir, "**b/*jetstream-subscriber-pull-?.csv")
            df_dict.update(collect_message_info(
                root_dir, "**cl_3/*jetstream-subscriber-pull-?.csv"))

        for key, msg_map in df_dict.items():
            latency = {}
            for msg_size, df in msg_map.items():
                latency[msg_size] = (
                    df.loc[:, 'receivedTime'] - df.loc[:, 'sentTime']).values
            latency_map[key] = latency

        latency_all_map.update(latency_map)
    plot_bar_from_key_latency(root_dir, latency_all_map, "latency", match_keys,
                                title="", 
                                yscale='log', 
                                xtickslabels=xticks_labels,
                                figsize=(9, 4.8),
                                ylim=[10, 8*10**4])

    # for broker, _ in pub_throughput_map.items():
    #     broker_info = broker.split('-')
    #     if len(broker_info) != 1 and broker_info[-2] == 'cl':
    #         continue

    #     latency_map = {}
    #     if broker_info[0] == 'kafka' and broker_info[2] == '1':
    #         match_keys = ['kafka-acks-1', 'kafka-acks-1-cl-3']
    #         df_dict = collect_latency(
    #             root_dir, "**acks_1/*kafka-subscriber-?-?.csv")
    #         df_dict.update(collect_latency(
    #             root_dir, "**acks_1_cl_3/*kafka-subscriber-?-?.csv"))
    #     elif broker_info[0] == 'kafka' and broker_info[2] == 'all':
    #         match_keys = ['kafka-acks-all', 'kafka-acks-all-cl-3']
    #         df_dict = collect_latency(
    #             root_dir, "**acks_all/*kafka-subscriber-?-?.csv")
    #         df_dict.update(collect_latency(
    #             root_dir, "**acks_all_cl_3/*kafka-subscriber-?-?.csv"))
    #     elif broker_info[0] == 'jetstream':
    #         match_keys = ['jetstream', 'jetstream-cl-3']
    #         df_dict = collect_latency(
    #             root_dir, "**b/*jetstream-subscriber-pull-?.csv")
    #         df_dict.update(collect_latency(
    #             root_dir, "**cl_3/*jetstream-subscriber-pull-?.csv"))

    #     for key, msg_map in df_dict.items():
    #         latency = {}
    #         for msg_size, df in msg_map.items():
    #             latency[msg_size] = (
    #                 df.loc[:, 'receivedTime'] - df.loc[:, 'sentTime']).values
    #         latency_map[key] = latency

    #     plot_bar_from_key_latency(root_dir, latency_map, "latency", match_keys,
    #                               title=broker+": レプリケーション構成でのレイテンシー", yscale='log', xtickslabels=['latency-single', 'latency-replication'])


####################################################################

def plot_tool_and_notool():
    """
    ツール有とツール無の比較用プロット
    """
    root_dir = 'results/2022_0609_0131_tool_and_notool'
    pub_throughput_map = collect_throughput(
        root_dir, "**/*publisher*throughput*")
    pub_kafka_map = {}
    pub_kafka_map['kafka-100B'] = {
        'no tool': pub_throughput_map['kafka-acks-0-notool']['100b'],
        'tool': pub_throughput_map['kafka-acks-0']['100b']
    }
    pub_kafka_map['kafka-1KB'] = {
        'no tool': pub_throughput_map['kafka-acks-0-notool']['1kb'],
        'tool': pub_throughput_map['kafka-acks-0']['1kb']
    }
    pub_kafka_map['kafka-16KB'] = {
        'no tool': pub_throughput_map['kafka-acks-0-notool']['16kb'],
        'tool': pub_throughput_map['kafka-acks-0']['16kb']
    }
    pub_nats_map = {}
    pub_nats_map['nats-100B'] = {
        'no tool': pub_throughput_map['nats-notool']['100b'],
        'tool': pub_throughput_map['nats']['100b']
    }
    pub_nats_map['nats-1KB'] = {
        'no tool': pub_throughput_map['nats-notool']['1kb'],
        'tool': pub_throughput_map['nats']['1kb']
    }
    pub_nats_map['nats-16KB'] = {
        'on VM': pub_throughput_map['nats-notool']['16kb'],
        'on Tool': pub_throughput_map['nats']['16kb']
    }
    plot_bar_from_key(root_dir, pub_kafka_map, "pub", [], res_lower_limit)
    plot_bar_from_key(root_dir, pub_nats_map, "pub", [], res_lower_limit)

    kafka_df_list = collect_message_info(root_dir, "**/*kafka-subscriber-?-?.csv")
    nats_df_list = collect_message_info(root_dir, "**/*nats-subscriber-?.csv")
    kafka_latency_map = {}
    kafka_latency_map['kafka-100B'] = {
        'no tool': (kafka_df_list['kafka-acks-0-notool']['100b'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0-notool']['100b'].loc[:, 'sentTime']).values,
        'tool': (kafka_df_list['kafka-acks-0']['100b'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0']['100b'].loc[:, 'sentTime']).values
    }
    kafka_latency_map['kafka-1KB'] = {
        'no tool': (kafka_df_list['kafka-acks-0-notool']['1kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0-notool']['1kb'].loc[:, 'sentTime']).values,
        'tool': (kafka_df_list['kafka-acks-0']['1kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0']['1kb'].loc[:, 'sentTime']).values
    }
    kafka_latency_map['kafka-16KB'] = {
        'no tool': (kafka_df_list['kafka-acks-0-notool']['16kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0-notool']['16kb'].loc[:, 'sentTime']).values,
        'tool': (kafka_df_list['kafka-acks-0']['16kb'].loc[:, 'receivedTime']-kafka_df_list['kafka-acks-0']['16kb'].loc[:, 'sentTime']).values
    }
    plot_bar_from_key_latency(root_dir, kafka_latency_map, "latency", [])

    nats_latency_amp = {}
    nats_latency_amp['nats-100B'] = {
        'no tool': (nats_df_list['nats-notool']['100b'].loc[:, 'receivedTime']-nats_df_list['nats-notool']['100b'].loc[:, 'sentTime']).values,
        'tool': (nats_df_list['nats']['100b'].loc[:, 'receivedTime']-nats_df_list['nats']['100b'].loc[:, 'sentTime']).values
    }
    nats_latency_amp['nats-1KB'] = {
        'no tool': (nats_df_list['nats-notool']['1kb'].loc[:, 'receivedTime']-nats_df_list['nats-notool']['1kb'].loc[:, 'sentTime']).values,
        'tool': (nats_df_list['nats']['1kb'].loc[:, 'receivedTime']-nats_df_list['nats']['1kb'].loc[:, 'sentTime']).values
    }
    nats_latency_amp['nats-16KB'] = {
        'no tool': (nats_df_list['nats-notool']['16kb'].loc[:, 'receivedTime']-nats_df_list['nats-notool']['16kb'].loc[:, 'sentTime']).values,
        'tool': (nats_df_list['nats']['16kb'].loc[:, 'receivedTime']-nats_df_list['nats']['16kb'].loc[:, 'sentTime']).values
    }
    plot_bar_from_key_latency(root_dir, nats_latency_amp, "latency", [])

####################################################################

def get_info_from_dir(dir_path):
    dir_info = os.path.basename(dir_path).split('_')
    broker = dir_info[2]
    if broker == 'kafka':
        broker += '-' + dir_info[6] + '-' + dir_info[7]
    return broker

def convert_logs(log_file):
    with open(log_file) as f:
        j = []
        lines = f.read().splitlines()
        log = {}
        for l in lines:
            index = l.find('|')
            dct = json.loads(l[index+2:])
            if "running" in dct["msg"]:
                log = {}
                dt = datetime.strptime(dct["time"], "%Y-%m-%dT%H:%M:%S%z")
                dt = dt.astimezone(timezone(timedelta(hours=+9)))
                log["start_time"] = time.mktime(dt.timetuple())
            elif "stopping" in dct["msg"]:
                dt = datetime.strptime(dct["time"], "%Y-%m-%dT%H:%M:%S%z")
                dt = dt.astimezone(timezone(timedelta(hours=+9)))
                log["end_time"] = time.mktime(dt.timetuple())
            else:
                raise RuntimeError(f"Unexpected error : {dct['msg'] }")
            # j.append(json.loads(l[index+2:]))
            # dt = datetime.strptime(j[-1]['time'], "%Y-%m-%dT%H:%M:%S%z")
            # j[-1]['time'] = dt.strftime('%Y-%m-%d %H:%M:%S%z')
    # json_file = os.path.join(os.path.dirname(log_file), os.path.splitext(os.path.basename(log_file))[0]+'.json')
    # with open(json_file, mode="w") as f:
    #     json.dump(result, f, indent=4)
    return log

def eval_results(root_dir, pub_throughput, sub_throughput, latency, pumba_log, broker):
    normal_pub_throughput = pub_throughput[(pumba_log['start_time']-20<pub_throughput[:,0]) & (pub_throughput[:,0]<pumba_log['start_time']), :]
    normal_sub_throughput = sub_throughput[(pumba_log['start_time']-20<sub_throughput[:,0]) & (sub_throughput[:,0]<pumba_log['start_time']), :]
    normal_latency = latency[(pumba_log['start_time']-20<latency[:,0]) & (latency[:,0]<pumba_log['start_time']), :]

    fault_pub_throughput = pub_throughput[(pumba_log['start_time']<pub_throughput[:,0]) & (pub_throughput[:,0]<pumba_log['end_time']), :]
    fault_sub_throughput = sub_throughput[(pumba_log['start_time']<sub_throughput[:,0]) & (sub_throughput[:,0]<pumba_log['end_time']), :]
    fault_latency = latency[(pumba_log['start_time']<latency[:,0]) & (latency[:,0]<pumba_log['end_time']), :]

    fig, ax = plt.subplots()
    ax.plot(fault_latency[:, 0], fault_latency[:, 1])
    ax.plot(normal_latency[:, 0], normal_latency[:, 1])
    fig.savefig(os.path.join(root_dir, broker+'_latency_check.png'))
    ax.cla()

    ax.plot(fault_pub_throughput[:, 0], fault_pub_throughput[:, 2])
    ax.plot(normal_pub_throughput[:, 0], normal_pub_throughput[:, 2])
    fig.savefig(os.path.join(root_dir, broker+'_pub_check.png'))
    ax.cla()

    ax.plot(fault_sub_throughput[:, 0], fault_sub_throughput[:, 2])
    ax.plot(normal_sub_throughput[:, 0], normal_sub_throughput[:, 2])
    fig.savefig(os.path.join(root_dir, broker+'_sub_check.png'))
    ax.cla()

    result = {
        'publisher':[
            normal_pub_throughput[:, 2].mean(),
            fault_pub_throughput[:, 2].mean()
        ],
        'subscriber':[
            normal_sub_throughput[:, 2].mean(),
            fault_sub_throughput[:, 2].mean()
        ],
        'latency':[
            normal_latency[:, 1].mean(),
            fault_latency[:, 1].mean()
        ],
    }
    return result

def plot_fault_injection():
    # root_dir = "results/2022_0615_0335_fault_injection_10mb_3mb_10msec"
    # root_dir = "results/2022_0615_0805_fault_injection_10mb_3mb_10msec"
    root_dir = "results/2022_0710_2050_fault_injection_10mb_3mb_20msec"

    dir_list = glob(root_dir+'/*')
    dir_list = sorted(dir_list)
    fault_results = {}

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    for dir_path in dir_list:
        if os.path.isfile(dir_path):
            continue
        broker = get_info_from_dir(dir_path)
        pumba_log_file = os.path.join(dir_path, 'delay1-0_delay1-0.log')
        pumba_log = convert_logs(pumba_log_file)
        csv_files = glob(os.path.join(dir_path, "*.csv"))
        for csv_f in csv_files:
            if re.search(r'publisher.*throughput\.csv', csv_f) is not None:
                pub_throughput = np.loadtxt(csv_f, delimiter=',', skiprows=1, dtype=np.float64)
                pub_throughput[:, 2] /= 1000*1000
            elif re.search(r'subscriber.*throughput\.csv', csv_f) is not None:
                sub_throughput = np.loadtxt(csv_f, delimiter=',', skiprows=1, dtype=np.float64)
                sub_throughput[:, 2] /= 1000*1000
            elif re.search(r'[^throughput]\.csv', csv_f) is not None:
                message_df = pd.read_csv(csv_f)
                senttime_arr = message_df['sentTime'].values/1000
                recievedtime_arr = message_df['receivedTime'].values/1000
                latency = np.stack([senttime_arr-senttime_arr[0], recievedtime_arr-senttime_arr], axis=1)
            
        pub_throughput[:,0] -= senttime_arr[0]
        sub_throughput[:,0] -= senttime_arr[0]
        ax1.plot(pub_throughput[:,0], pub_throughput[:,2], label="pub throughput", color="C0")
        ax1.plot(sub_throughput[:,0], sub_throughput[:,2], label="sub throughput", color="C1")
        ax1.set_ylabel("Throughput[MB/sec]")
        ax1.set_xlabel('elapsed time[sec]')
        ax2.plot(latency[:,0], latency[:,1], label="latency", color="C2")
        ax2.set_ylabel("Latency[sec]")
        
        min_latency, max_latency = ax2.get_ylim()
        pumba_log["start_time"] -= senttime_arr[0]
        pumba_log["end_time"] -= senttime_arr[0]
        pumba_log["start_time"] += 14
        pumba_log["end_time"] += 7
        # ax2.axvline(pumba_log["start_time"], ymin=0, ymax=max_latency, color="r", label="Pumba start time", alpha=0.5)
        # ax2.axvline(pumba_log["end_time"], ymin=0, ymax=max_latency, color="r", label="Pumba end time", alpha=0.5)
        # ax2.axvline(pumba_log["start_time"]+14, ymin=0, ymax=max_latency, color="r", label="Estimated failure start time", linestyle="dashed")
        # ax2.axvline(pumba_log["end_time"]+7, ymin=0, ymax=max_latency, color="r", label="Estimated failure end time", linestyle="dashed")
        # ax2.axvline(pumba_log["start_time"], ymin=0, ymax=max_latency, color="r", alpha=0.5)
        # ax2.axvline(pumba_log["end_time"], ymin=0, ymax=max_latency, color="r", alpha=0.5)
        ax2.axvline(pumba_log["start_time"], ymin=min_latency, ymax=100, color="r", linestyle="dashed")
        ax2.axvline(pumba_log["end_time"], ymin=min_latency, ymax=100, color="r", linestyle="dashed")

        ax1.legend(loc='upper left')
        ax2.legend(loc='upper right')
        ax2.set_ylim((min_latency, max_latency))
        # ax2.set_yscale('log')
        fig.savefig(os.path.join(root_dir, broker+'.png'), bbox_inches='tight')
        ax1.cla()
        ax2.cla()

        fault_results[broker] = eval_results(dir_path, pub_throughput, sub_throughput, latency, pumba_log, broker)

    fig, ax = plt.subplots()
    x = []
    y_pub = []
    y_sub = []
    y_latency = []
    x_ticks = []
    x_ticks_labels = []
    i = 0
    header = []
    for broker, res in fault_results.items():
        x.extend([i, i+0.4])
        y_pub.extend(res['publisher'])
        y_sub.extend(res['subscriber'])
        y_latency.extend(res['latency'])

        x_ticks.append(i+0.2)
        x_ticks_labels.append(broker)
        i += 1

        header.extend([broker+"-normal", broker+"fault"])

    ax.bar(x[0::2], y_pub[0::2], width=0.4, label='normal')
    ax.bar(x[1::2], y_pub[1::2], width=0.4, label='fault')
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(x_ticks_labels)
    ax.legend()
    fig.savefig(os.path.join(root_dir, 'normal_and_fault_res_pub.png'), bbox_inches='tight')
    ax.cla()

    ax.bar(x[0::2], y_sub[0::2], width=0.4, label='normal')
    ax.bar(x[1::2], y_sub[1::2], width=0.4, label='fault')
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(x_ticks_labels)
    ax.legend()
    fig.savefig(os.path.join(root_dir, 'normal_and_fault_res_sub.png'), bbox_inches='tight')
    ax.cla()

    ax.bar(x[0::2], y_latency[0::2], width=0.4, label='normal')
    ax.bar(x[1::2], y_latency[1::2], width=0.4, label='fault')
    ax.set_xticks(x_ticks)
    ax.set_xticklabels(x_ticks_labels)
    ax.set_yscale('log')
    ax.legend()
    fig.savefig(os.path.join(root_dir, 'normal_and_fault_res_latency.png'), bbox_inches='tight')
    ax.cla()

    with open(os.path.join(root_dir, "normal_and_fault_res_pub.csv"), mode="w") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(y_pub)

    with open(os.path.join(root_dir, "normal_and_fault_res_sub.csv"), mode="w") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(y_sub)

    with open(os.path.join(root_dir, "normal_and_fault_res_latency.csv"), mode="w") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(y_latency)

####################################################################

if __name__ == '__main__':
    res_lower_limit = 100
    # plot_single(res_lower_limit)
    # plot_single_kafka()
    # plot_cluster(res_lower_limit)
    # plot_tool_and_notool(res_lower_limit)
    plot_fault_injection()
