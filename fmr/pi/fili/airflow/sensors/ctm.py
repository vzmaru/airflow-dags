#
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

from typing import Sequence

from airflow.sensors.base import BaseSensorOperator
from fmr.pi.fili.airflow.triggers.ctm import CtmConditionTrigger
from airflow.utils.context import Context


class CtmConditionSensorAsync(BaseSensorOperator):
    """
    Waits until the specified datetime, deferring itself to avoid taking up
    a worker slot while it is waiting.

    It is a drop-in replacement for CtmConditionSensor.

    :param target_time: datetime after which the job succeeds. (templated)
    """

    template_fields: Sequence[str] = ("event_name",)

    def __init__(self, *, event_name: str, **kwargs) -> None:
        super().__init__(**kwargs)

        # self.target_time can't be a datetime object as it is a template_field
        if isinstance(event_name, str):
            self.event_name = event_name
        else:
            raise TypeError(
                f"Expected str type for target_time. Got {type(event_name)}"
            )

    def execute(self, context: Context):
        self.defer(
            trigger=CtmConditionTrigger(event_name=self.event_name),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):
        """Callback for when the trigger fires - returns immediately."""
        return None
