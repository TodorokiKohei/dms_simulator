import argparse
import glob
import os
import shutil
import json
from datetime import datetime, timedelta
from matplotlib.figure import Figure
from matplotlib.axes import Axes

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def collect_results():
    csv_files = glob.glob('exec_dir/controller/**/*.csv')
    for f in csv_files:
        shutil.copy(f, os.path.join(result_dir, os.path.basename(f)))
    log_files = glob.glob('exec_dir/controller/**/*.log')
    for f in log_files:
        shutil.copy(f, os.path.join(result_dir, os.path.basename(f)))
        
    shutil.copy(template_file, os.path.join(result_dir, template_file))

    internal_ip_files = glob.glob("exec_dir/controller/**/*internal_ip")
    for f in internal_ip_files:
        shutil.copy(f, os.path.join(result_dir, os.path.basename(f)))
    

def convert_logs():
    log_files = glob.glob(os.path.join(result_dir, '*.log'))
    for f in log_files:
        with open(f) as ff:
            j = []
            lines = ff.read().splitlines()
            result = {}
            for l in lines:
                index = l.find('|')
                dct = json.loads(l[index+2:])
                if "running" in dct["msg"]:
                    result[dct["name"]] = {}
                    result[dct["name"]]["start_time"] = dct["time"]
                elif "stopping" in dct["msg"]:
                    result[dct["name"]]["end_time"] = dct["time"]
                else:
                    raise RuntimeError(f"Unexpected error : {dct['msg'] }")
                # j.append(json.loads(l[index+2:]))
                # dt = datetime.strptime(j[-1]['time'], "%Y-%m-%dT%H:%M:%S%z")
                # j[-1]['time'] = dt.strftime('%Y-%m-%d %H:%M:%S%z')
        json_file = os.path.join(result_dir, os.path.splitext(os.path.basename(f))[0]+'.json')
        with open(json_file, mode="w") as ff:
            json.dump(result, ff, indent=4)


def add_failure_time(fig: Figure, ax: Axes, failure_logs):
    for failure_log in failure_logs:
        ax.axvline(failure_log["start_time"], ymin=0, ymax=1, color="r", label="Pumba start time", linestyle="dashed")
        ax.axvline(failure_log["end_time"], ymin=0, ymax=1, color="r", label="Pumba end time", linestyle="dashed")
        ax.axvline(failure_log["start_time"]+14, ymin=0, ymax=1, color="r", label="Estimated failure start time")
        ax.axvline(failure_log["end_time"]+7, ymin=0, ymax=1, color="r", label="Estimated failure end time")
        # ax.axvspan(failure_log["start_time"], failure_log["end_time"], color="gray", alpha=0.3)
        ax.legend()


def plot_results():
    publisher_file = os.path.join(result_dir, 'pub-1_client_0_results.csv')
    df_pub = pd.read_csv(publisher_file, parse_dates=[0])
    subscriber_file = os.path.join(result_dir, 'sub-1_client_0_results.csv')
    df_sub = pd.read_csv(subscriber_file, parse_dates=[0])

    df_pub['LOGTIME'] = df_pub['LOGTIME'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
    df_sub['LOGTIME'] = df_sub['LOGTIME'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')

    # df_pub.drop(0, axis=0)
    # df_sub.drop(0, axis=0)

    # 障害ログ
    log_files = glob.glob(os.path.join(result_dir, "*.json"))
    failure_logs = []
    for f in log_files:
        with open(f) as ff:
            log = json.load(ff)     
            for key, val in log.items():       
                start_time = (pd.to_datetime(val["start_time"], format="%Y-%m-%dT%H:%M:%S%z").tz_convert("Asia/Tokyo") - df_pub['LOGTIME'][0]).total_seconds()
                end_time = (pd.to_datetime(val["end_time"], format="%Y-%m-%dT%H:%M:%S%z").tz_convert("Asia/Tokyo") - df_pub['LOGTIME'][0]).total_seconds()
                container = key.split(".")[0]
                failure_logs.append({
                    "container": container,
                    "start_time": start_time,
                    "end_time": end_time
                })

    fig, ax = plt.subplots()
    fig.subplots_adjust(left=0.14)

    # Publisherのレートを描画
    ax.plot((df_pub['LOGTIME']-df_pub['LOGTIME'][0]).dt.total_seconds(), df_pub['MESSAGE_BYTES_TOTAL'])
    ax.set_ylabel('publish rate [B/sec]')
    ax.set_xlabel('elapsed time [sec]')
    ax.set_title(title)
    add_failure_time(fig, ax, failure_logs)
    # plt.ylim([0, 160000])
    fig.savefig(os.path.join(result_dir, 'pub_rate.png'))
    ax.cla()

    # Subscriberのレートを描画
    ax.plot((df_sub['LOGTIME']-df_sub['LOGTIME'][0]).dt.total_seconds(), df_sub['MESSAGE_BYTES_TOTAL'])
    ax.set_ylabel('subscribe rate [B/sec]')
    ax.set_xlabel('elapsed time [sec]')
    ax.set_title(title)
    add_failure_time(fig, ax, failure_logs)
    # plt.ylim([0, 160000])
    fig.savefig(os.path.join(result_dir, 'sub_rate.png'))
    ax.cla()

    messages_file = os.path.join(result_dir, 'sub-1_client_0_messages.csv')
    if not os.path.exists(messages_file):
        return
    df_msg = pd.read_csv(messages_file, parse_dates=[2,3])

    # TimeZoneの設定
    df_msg['sent_time'] = df_msg['sent_time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')
    df_msg['receive_time'] = df_msg['receive_time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Tokyo')

    # 時刻から経過時間に変更
    df_msg['time_from_sent_start'] = (df_msg['sent_time'] - df_msg['sent_time'][0]).dt.total_seconds()
    df_msg['time_from_recv_start'] = (df_msg['receive_time'] - df_msg['receive_time'][0]).dt.total_seconds()

    # 1行目を削除
    df_msg.drop(0, axis=0)

    # レイテンシーを描画
    latency = (df_msg['receive_time'] - df_msg['sent_time']).dt.total_seconds()
    ax.plot(df_msg['time_from_sent_start'], latency, marker="o", markersize=4, linestyle="None")
    ax.set_ylabel('latency [sec]')
    ax.set_xlabel('elapsed time [sec]')
    ax.set_title(title)
    add_failure_time(fig, ax, failure_logs)
    # plt.ylim([0, 0.35])
    fig.savefig(os.path.join(result_dir, 'latency.png'))
    ax.cla()

    # Publisherのメッセージの送信間隔を描画
    pub_message_diff = df_msg['sent_time'].diff().dropna().dt.total_seconds()
    ax.plot(df_msg['time_from_sent_start'].iloc[:-1], pub_message_diff)
    ax.set_ylabel('message interval [sec]')
    ax.set_xlabel('elapsed time [sec]')
    ax.set_title(title)
    add_failure_time(fig, ax, failure_logs)
    fig.savefig(os.path.join(result_dir, 'pub_message_diff.png'))
    ax.cla()

    # Subscriberのメッセージの受信間隔を描画
    sub_message_diff = df_msg['receive_time'].diff().dropna().dt.total_seconds()
    ax.plot(df_msg['time_from_recv_start'].iloc[:-1], sub_message_diff)
    ax.set_ylabel('message interval [sec]')
    ax.set_xlabel('elapsed time [sec]')
    ax.set_title(title)
    add_failure_time(fig, ax, failure_logs)
    # plt.ylim([0, 5])
    fig.savefig(os.path.join(result_dir, 'sub_message_diff.png'))
    ax.cla()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir')
    parser.add_argument('--file')
    parser.add_argument('--title')
    args = parser.parse_args()
    if args.dir is None:
        result_dir = os.path.join('results', datetime.now().strftime("%Y%m%d_%H%M"))
    else:
        result_dir = os.path.join('results', args.dir)
    os.makedirs(result_dir, exist_ok=True)

    if args.title is None:
        title = ""
    else:
        title = args.title

    if args.file is None:
        template_file = "template.yml"
    else:
        template_file = args.file
    collect_results()
    convert_logs()
    plot_results()
