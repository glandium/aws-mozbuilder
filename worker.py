# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import time
from config import Config
from util import cached_property

from mozillapulse.publishers import GenericPublisher
from mozillapulse.consumers import GenericConsumer
from mozillapulse.config import PulseConfiguration
from mozillapulse.messages.base import GenericMessage


def PulseExchange(cls, config, **kwargs):
    return cls(PulseConfiguration(**kwargs),
        'exchange/%s/%s' % (config.pulse_user, config.type),
        user=config.pulse_user, password=config.pulse_password, **kwargs)


class LogMessage(GenericMessage):
    def __init__(self):
        super(LogMessage, self).__init__()
        self.routing_parts.append('log')


class LoggingHandler(logging.Handler):
    def __init__(self):
        config = Config()
        self._queue = PulseExchange(GenericPublisher, config)
        self._instanceId = config.instanceId
        logging.Handler.__init__(self)
        self._dummy_record = logging.LogRecord('', 0, '', 0, '', (), None)

    def emit(self, record):
        if not self._queue:
            return
        m = LogMessage()
        m.set_data('level', record.levelname)
        m.set_data('name', record.name)
        m.set_data('instanceId', self._instanceId)
        m.set_data('message', record.msg)
        # Record any extra data attached to the record.
        for key, value in record.__dict__.items():
            if key not in self._dummy_record.__dict__:
                m.set_data(key, value)
        self._queue.publish(m)


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
        import uuid
        return PulseExchange(GenericConsumer, self._config,
            applabel=str(uuid.uuid4()))

    def shutdown(self):
        if not self._running:
            return
        self._running = False
        self._logger.warning('Shutting down worker')

    def run(self):
        if not self._running:
            return

        self._queue.configure(topic=['#'], callback=self._handle_message)
        self._queue.listen()
