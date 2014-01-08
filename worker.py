# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import time
from boto.sqs.jsonmessage import JSONMessage
from botohelpers import SQSConnection
from config import Config
from util import cached_property


class SQSLoggingHandler(logging.Handler):
    def __init__(self):
        config = Config()
        sqs_conn = SQSConnection()
        self._queue = sqs_conn.get_queue('%s-logs' % config.type)
        self._instanceId = config.instanceId
        logging.Handler.__init__(self)
        self._dummy_record = logging.LogRecord('', 0, '', 0, '', (), None)

    def emit(self, record):
        if not self._queue:
            return
        m = JSONMessage()
        m['level'] = record.levelname
        m['name'] = record.name
        m['instanceId'] = self._instanceId
        m['message'] = record.msg
        # Record any extra data attached to the record.
        for key, value in record.__dict__.items():
            if key not in self._dummy_record.__dict__:
                m[key] = value
        self._queue.write(m)


class Worker(object):
    def __init__(self, revision=None):
        self._config = Config()
        self._idle_since = time.time()
        self._logger = logging.getLogger('Worker')
        if not self._queue:
            message = 'No job queue named %s' % self._queue_name
            self._logger.error(message)
            raise Exception(message)
        if revision:
            self._logger.warning('Starting worker revision %s' % revision)
        else:
            self._logger.warning('Starting worker')
        self._running = True

    @cached_property
    def _queue(self):
        queue = SQSConnection().get_queue(self._queue_name)
        if queue:
            queue.set_message_class(JSONMessage)
        return queue

    def shutdown(self):
        if not self._running:
            return
        self._running = False
        self._logger.warning('Shutting down worker')

    def run(self):
        if not self._running:
            return

        m = self._queue.get_messages(
            num_messages=1,
            visibility_timeout=7200,
            attributes='SentTimestamp',
            wait_time_seconds=20,
        )
        if len(m) != 1:
            if self._config.max_idle and \
                    time.time() - self._idle_since > self._config.max_idle:
                self.shutdown()
            return

        m = m[0]
        try:
            self._handle_message(m)
            self._queue.delete_message(m)
        except:
            import traceback
            self._logger.error(traceback.format_exc())
            m.change_visibility(0)
        self._idle_since = time.time()
