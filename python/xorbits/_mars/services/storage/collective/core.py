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
import logging
from typing import List

import xoscar as mo
from xoscar.collective.core import RankActor, broadcast, new_group

from ....storage import StorageLevel

logger = logging.getLogger(__name__)


class CollectiveActor(mo.StatelessActor):
    def __init__(
        self,
        rank_actor_ref: mo.ActorRefType[RankActor],
        storage_handler_ref,
    ):
        super().__init__()
        self._rank_ref = rank_actor_ref
        self._storage_handler_ref = storage_handler_ref

    @classmethod
    def gen_uid(cls):
        return "CollectiveActor"

    @classmethod
    def default_uid(cls):
        return "CollectiveActor"

    def rank(self):
        return self._rank_ref.rank()

    async def new_group(self, ranks: List[int]):
        return await new_group(ranks)

    async def broadcast(
        self,
        session_id: str,
        root_data_key: str,
        data_size: int,
        root: int = 0,
        group_name: str = "default",
    ):
        r = await self.rank()
        if root == r:
            reader = await self._storage_handler_ref.open_reader(
                session_id, root_data_key
            )
            buf = reader.buffer
            logger.debug(
                f"Open reader successfully rank: {r}, data key: {root_data_key}"
            )
            await broadcast(buf, buf, root=root, group_name=group_name)
            logger.debug(
                f"Broadcast successfully!! rank: {r}, data key: {root_data_key}"
            )
            result = await self._storage_handler_ref.get(session_id, root_data_key)
            logger.debug(
                f"In broadcast actor: after broadcast: rank: {r}, data key: {root_data_key}, data: {result}"
            )
        else:
            await self._storage_handler_ref.request_quota_with_spill(
                StorageLevel.MEMORY, data_size
            )
            logger.debug(
                f"Request quota successfully rank: {r}, data key: {root_data_key}"
            )
            writer = await self._storage_handler_ref.open_writer(
                session_id,
                root_data_key,
                data_size,
                StorageLevel.MEMORY,
                request_quota=False,
                band_name="numa-0",
            )
            logger.debug(
                f"Open writer successfully rank: {r}, data key: {root_data_key}"
            )
            buf = writer.buffer
            await broadcast(None, buf, root=root, group_name=group_name)
            logger.debug(
                f"Broadcast successfully!! rank: {r}, data key: {root_data_key}"
            )
            await writer.close()
            result = await self._storage_handler_ref.get(session_id, root_data_key)
            logger.debug(
                f"In broadcast actor: after broadcast: rank: {r}, data key: {root_data_key}, data: {result}"
            )
