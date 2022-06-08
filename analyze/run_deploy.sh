#!/bin/bash

template_file=$1

cd ../
source init_config.sh
source venv/bin/activate
python dms_simulater.py --mode remove-results --file "${template_file}"
python dms_simulater.py --mode init --file "${template_file}"
python dms_simulater.py --mode deploy --file "${template_file}"