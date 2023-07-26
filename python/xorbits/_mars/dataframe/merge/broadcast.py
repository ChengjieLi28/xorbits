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
import logging
from typing import Dict, List

import xoscar as xo

from ...serialization.serializables import ListField
from ..operands import DataFrameOperand, DataFrameOperandMixin

logger = logging.getLogger(__name__)


class DataFrameBroadcast(DataFrameOperand, DataFrameOperandMixin):
    target_keys = ListField("target_keys")

    def __init__(self, output_types=None, **kw):
        super().__init__(_output_types=output_types, **kw)

    @classmethod
    async def _do_broadcast(
        cls,
        session_id: str,
        data_key: str,
        data_size: int,
        addrs: List[str],
        supervisor_addr: str,
    ):
        lock_ref = await xo.actor_ref(
            address=supervisor_addr, uid="CollectiveLockActor"
        )
        # root_addr = addrs[0]
        # root_ref = await xo.actor_ref(address=root_addr, uid="CollectiveActor")
        # root_rank = await root_ref.rank()
        refs = []
        ranks = []
        for addr in addrs:
            ref = await xo.actor_ref(address=addr, uid="CollectiveActor")
            rank = await ref.rank()
            ranks.append(rank)
            refs.append(ref)

        # new_group
        root_rank = ranks[0]
        ranks = sorted(ranks)
        pg_tasks = []
        for ref in refs:
            pg_tasks.append(ref.new_group(ranks))

        pg_names = await asyncio.gather(*pg_tasks)
        await lock_ref.get_lock(pg_names[0])

        # tasks = [
        #     root_ref.broadcast(
        #         session_id,
        #         data_key,
        #         data_size,
        #         ranks,
        #         root_rank
        #     )
        # ]
        tasks = []
        for ref in refs:
            tasks.append(
                ref.broadcast(
                    session_id, data_key, data_size, ranks, root_rank, pg_names[0]
                )
            )
        await asyncio.gather(*tasks)
        await lock_ref.release_lock(pg_names[0])

    @classmethod
    def execute(cls, ctx, op: "DataFrameBroadcast"):
        to_broadcast_chunk_key = op.inputs[0].key
        to_broadcast_chunk = ctx[to_broadcast_chunk_key]
        recv_chunk_keys = op.target_keys
        metas: List[Dict] = ctx.get_chunks_meta(
            [to_broadcast_chunk_key, *recv_chunk_keys], fields=["store_size", "bands"]
        )
        data_size = metas[0]["store_size"]
        session_id = ctx.session_id
        data_key = to_broadcast_chunk_key
        addrs = [meta["bands"][0][0] for meta in metas]
        root_addr = addrs[0]
        other_addrs = set(addrs[1:])
        if root_addr in other_addrs:
            other_addrs.remove(root_addr)
        if other_addrs:
            supervisor_addr = ctx.get_supervisor_addresses()[0]
            ctx._call(
                cls._do_broadcast(
                    session_id,
                    data_key,
                    data_size,
                    [root_addr, *other_addrs],
                    supervisor_addr,
                )
            )
            # fut = asyncio.run_coroutine_threadsafe(
            #     cls._do_broadcast(session_id, data_key, data_size, [root_addr, *other_addrs], supervisor_addr),
            #     loop=ctx._loop)
            # fut.result()
        ctx[op.outputs[0].key] = to_broadcast_chunk
