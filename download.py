import subprocess
import time
import sys

sleep_time = sys.argv[1] if sys.argv[1] else 0
subprocess.check_call('mkdir -p data/itv-github/landing/ghactivity', shell=True)
for d in ['2021-01-13', '2021-01-14', '2021-01-15']:
    for h in range(0, 24):
        time.sleep(sleep_time)
        subprocess. \
            check_call(
                f'wget https://data.gharchive.org/{d}-{h}.json.gz -O data/itv-github/landing/ghactivity/{d}-{h}.json.gz', 
                shell=True
            )
