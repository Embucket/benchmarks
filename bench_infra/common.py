import os
import json
import subprocess
import time
import urllib.request
import urllib.error


def get_ec2_metadata():
    try:
        token_url = 'http://169.254.169.254/latest/api/token'
        token_req = urllib.request.Request(
            token_url, headers={'X-aws-ec2-metadata-token-ttl-seconds': '21600'}, method='PUT'
        )
        with urllib.request.urlopen(token_req, timeout=1) as response:
            token = response.read().decode('utf-8')

        meta_url = 'http://169.254.169.254/latest/meta-data/instance-type'
        meta_req = urllib.request.Request(
            meta_url, headers={'X-aws-ec2-metadata-token': token}
        )
        with urllib.request.urlopen(meta_req, timeout=1) as response:
            return response.read().decode('utf-8').strip()
    except:
        return "unknown"


def drop_os_caches():
    print('--- Flushing OS buffers ---')
    try:
        subprocess.run(["sudo", "sync"], check=True)
        subprocess.run("echo 3 | sudo tee /proc/sys/vm/drop_caches", shell=True, check=True)
        time.sleep(3)
    except Exception as e:
        print(f"Warning: Could not drop caches: {e}")


def save_results(results_dict, output_file):
    results_dict['system_info'] = {
        'ec2_instance': get_ec2_metadata(),
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    }

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, 'w') as f:
        json.dump(results_dict, f, indent=4)
    print(f"\n>>> Results saved to {output_file}")