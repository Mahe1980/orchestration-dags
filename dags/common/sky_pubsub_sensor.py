# -*- coding: utf-8 -*-
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

from airflow.contrib.hooks.gcp_pubsub_hook import PubSubHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import logging
from datetime import datetime


class SkyPubSubSensor(BaseSensorOperator):
    """Pulls messages from a PubSub subscription and passes them through XCom.

    This sensor operator will pull up to ``max_messages`` messages from the
    specified PubSub subscription. When the subscription returns messages,
    the poke method's criteria will be fulfilled and the messages will be
    returned from the operator and passed through XCom for downstream tasks.

    If ``ack_messages`` is set to True, messages will be immediately
    acknowledged before being returned, otherwise, downstream tasks will be
    responsible for acknowledging them.

    ``project`` and ``subscription`` are templated so you can use
    variables in them.
    """
    template_fields = ['project', 'subscription']
    ui_color = '#ff7f50'

    @apply_defaults
    def __init__(
            self,
            project,
            subscription,
            table_names,
            timestamp,
            max_messages=40,
            return_immediately=False,
            ack_messages=False,
            gcp_conn_id='google_cloud_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        :param project: the GCP project ID for the subscription (templated)
        :type project: str
        :param subscription: the Pub/Sub subscription name. Do not include the
            full subscription path.
        :type subscription: str
        :param table_names: table names to filter the pub/sub messages
        :type table_names: list
        :param max_messages: The maximum number of messages to retrieve per
            PubSub pull request
        :type max_messages: int
        :param return_immediately: If True, instruct the PubSub API to return
            immediately if no messages are available for delivery.
        :type return_immediately: bool
        :param ack_messages: If True, each message will be acknowledged
            immediately rather than by any downstream tasks
        :type ack_messages: bool
        :param gcp_conn_id: The connection ID to use connecting to
            Google Cloud Platform.
        :type gcp_conn_id: str
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request
            must have domain-wide delegation enabled.
        :type delegate_to: str
        """
        super(SkyPubSubSensor, self).__init__(*args, **kwargs)

        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project = project
        _, self.subscription = subscription.split('.')
        self.max_messages = max_messages
        self.return_immediately = return_immediately
        self.ack_messages = ack_messages
        self.table_names = table_names
        self.timestamp = timestamp
        self.tables = {}
        self._messages = None

    def execute(self, context):
        """Overridden to allow messages to be passed"""
        super(SkyPubSubSensor, self).execute(context)
        return self.tables

    def poke(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        self._messages = list()
        while True:
            messages = hook.pull(self.project, self.subscription, self.max_messages, self.return_immediately)
            if messages:
                self._messages += messages
            else:
                break
        for message in self._messages:
            table = message['message']['attributes']['tableId']
            if any(substring.lower() in table.lower() for substring in self.table_names):
                if table not in self.tables:
                    if 'ok_file' in message['message']['attributes']:
                        ok_file = message['message']['attributes']['ok_file']
                    else:
                        ok_file = None

                    logging.info("table to update is {}".format(table))
                    self.tables[table] = {
                        'created': datetime.strptime(message['message']['attributes']['eventTime'], self.timestamp),
                        'ack_ids': set(),
                        'ok_file': ok_file,
                        'current_time': datetime.utcnow()
                    }
                self.tables[table]['ack_ids'].add(message['ackId'])
        logging.info("Found {} tables to load.".format(len(self.tables)))

        if self._messages and self.ack_messages:
            if self.ack_messages:
                ack_ids = [m['ackId'] for m in self._messages if m.get('ackId')]
                hook.acknowledge(self.project, self.subscription, ack_ids)
        return self._messages
