# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import datetime
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException


class CtmConditionTrigger(BaseTrigger):
    """
    A trigger that fires exactly once, at the given datetime, give or take
    a few seconds.

    The provided datetime MUST be in UTC.
    """

    def __init__(self, event_name: str):
        super().__init__()
        if not isinstance(event_name, str):
            raise TypeError(f"Expected str type for event_name. Got {type(event_name)}")
        # Make sure it's in UTC
        else:
            self.event_name = event_name
        self.hook = HttpHook(method="GET", http_conn_id="ctm")

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return ("fmr.pi.fili.airflow.triggers.ctm.CtmConditionTrigger", {"event_name": self.event_name})

    async def run(self):
        """
        Simple time delay loop until the relevant time is met.

        We do have a two-phase delay to save some cycles, but sleeping is so
        cheap anyway that it's pretty loose. We also don't just sleep for
        "the number of seconds until the time" in case the system clock changes
        unexpectedly, or handles a DST change poorly.
        """
        while True:
            try:
                response = self.hook.run(
                    "run/events/"+self.event_name,
                )

                if len(response.json()) > 0:
                    break
                else:
                    await asyncio.sleep(15)
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    return False

                raise exc

        yield TriggerEvent(self.event_name)

