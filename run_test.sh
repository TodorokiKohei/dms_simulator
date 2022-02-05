#!/bin/bash

source init_config.sh
source venv/bin/activate
python dms_simulater.py --mode test --file template_test.yml
python plot_results.py --dir $1 --file template_test.yml