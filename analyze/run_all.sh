#!/bin/bash

template_file=$1
broker=$2
suffix=$3

cd ../
source init_config.sh
source venv/bin/activate
python dms_simulater.py --mode remove-results --file "${template_file}"
python dms_simulater.py --mode run --file "${template_file}"
# python dms_simulater.py --mode test --file "${template_file}"

cd analyze
python collect_result.py --broker "${broker}" --file "${template_file}" --res-suffix "${suffix}"