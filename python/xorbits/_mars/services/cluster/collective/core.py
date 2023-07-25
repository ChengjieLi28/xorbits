# Copyright 2022-2023 XProbe Inc.
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
import asyncio
from typing import Any

import xoscar as xo


class CollectiveLockActor(xo.Actor):
    def __init__(self):
        super().__init__()
        self._lock_table = {}
        self.cnt = 0

    @classmethod
    def default_uid(cls):
        return "CollectiveLockActor"

    async def get_lock(self, key: Any):
        if key not in self._lock_table:
            self._lock_table[key] = asyncio.Lock()
        yield self._lock_table[key].acquire()
        # print(f'Key {key} ready to sleep! Seconds: {self.cnt * 0.2}')
        # await asyncio.sleep(self.cnt * 0.2)
        # print(f'Key {key} sleep done! Seconds: {self.cnt * 0.2}')
        # self.cnt += 1

    def release_lock(self, key: Any):
        self._lock_table[key].release()
