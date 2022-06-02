import argparse
import glob
import os
import shutil
from datetime import datetime


def collect_results_from_broker(broker:str, template_file:str):
    result_files = glob.glob(f"../exec_dir/controller/{broker}/**/*", recursive=True)
    for f in result_files:
        if (os.path.isfile(f)):
            parent_dirname = os.path.basename(os.path.dirname(os.path.dirname(f)))
            shutil.copy(f, os.path.join(result_dir, parent_dirname+os.path.basename(f)))

    shutil.copy(os.path.join("..", template_file), os.path.join(result_dir, os.path.basename(template_file)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--broker', required=True)
    parser.add_argument('--file', required=True)
    parser.add_argument('--res-suffix')
    args = parser.parse_args()

    result_dir = os.path.join('results', f"{args.broker}_" + datetime.now().strftime("%Y%m%d_%H%M") + "_" + args.res_suffix)
    os.makedirs(result_dir, exist_ok=True)

    collect_results_from_broker(args.broker, args.file)