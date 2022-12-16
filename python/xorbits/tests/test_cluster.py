# Copyright 2022 XProbe Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import sys
import tempfile
import time

import psutil

from .. import init
from .._mars.utils import get_next_port
from ..core.api import API

CONFIG_CONTENT = b"""\
"@inherits": "@default"
scheduling:
  mem_hard_limit: null"""
# 100 sec to timeout
TIMEOUT = 1000


def _terminate(pid: int):
    proc = psutil.Process(pid)
    sub_pids = [p.pid for p in proc.children(recursive=True)]
    proc.terminate()
    proc.wait(5)
    for p in sub_pids:
        try:
            proc = psutil.Process(p)
            proc.kill()
        except psutil.NoSuchProcess:
            continue


def test_cluster(dummy_df):
    port = get_next_port()
    web_port = get_next_port()
    supervisor_addr = f"127.0.0.1:{port}"
    web_addr = f"http://127.0.0.1:{web_port}"

    with tempfile.NamedTemporaryFile() as f:
        f.write(CONFIG_CONTENT)
        f.flush()

        supervisor_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "xorbits.supervisor",
                "-H",
                "127.0.0.1",
                "-p",
                str(port),
                "-w",
                str(web_port),
                "-f",
                f.name,
            ]
        )
        worker_process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "xorbits.worker",
                "-s",
                supervisor_addr,
                "-f",
                f.name,
            ]
        )

        try:
            for p in [supervisor_process, worker_process]:
                try:
                    retcode = p.wait(1)
                except subprocess.TimeoutExpired:
                    # supervisor & worker will run forever,
                    # timeout means everything goes well, at least looks well,
                    continue
                else:
                    if retcode:
                        std_err = p.communicate()[1].decode()
                        raise RuntimeError("Start cluster failed, stderr: \n" + std_err)

            start = time.time()
            is_timeout = True
            while time.time() - start <= TIMEOUT:
                try:
                    init(web_addr)
                except:  # noqa: E722  # nosec  # pylint: disable=bare-except
                    time.sleep(0.5)
                    continue
                api = API.create()
                if len(api.list_workers()) == 0:
                    time.sleep(0.5)
                    continue

                assert repr(dummy_df.foo.sum()) == "3"
                is_timeout = False
                break
            if is_timeout:
                raise TimeoutError
        finally:
            _terminate(worker_process.pid)
            _terminate(supervisor_process.pid)
