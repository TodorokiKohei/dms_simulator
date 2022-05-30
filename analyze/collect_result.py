import glob
import os
import shutil
from datetime import datetime

result_dir = 'results/' + datetime.now().strftime("%Y%m%d_%H%M")
os.makedirs(result_dir, exist_ok=True)

result_files = glob.glob("../exec_dir/controller/**/results/*", recursive=True)
for f in result_files:
    shutil.copy(f, os.path.join(result_dir, os.path.basename(f)))
