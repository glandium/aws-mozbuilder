# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto.sqs.message
import logging
import time
from botohelpers import SQSConnection
from config import Config


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
        m = boto.sqs.message.MHMessage()
        m['level'] = record.levelname
        m['name'] = record.name
        m['instanceId'] = self._instanceId
        m['message'] = record.msg
        # Record any extra data attached to the record.
        for key, value in record.__dict__.items():
            if key not in self._dummy_record.__dict__:
                m[key] = value
        self._queue.write(m)


class Job(object):
    def __init__(self, branch, changeset):
        self.branch = branch
        self.changeset = changeset

    @staticmethod
    def from_message(msg):
        assert isinstance(msg, boto.sqs.message.MHMessage)
        return Job(msg['branch'], msg['changeset'])

    def to_message(self):
        msg = boto.sqs.message.MHMessage()
        msg['branch'] = self.branch
        msg['changeset'] = self.changeset
        return msg


class Worker(object):
    def __init__(self, revision=None):
        self._config = Config()
        self._idle_since = time.time()
        self._logger = logging.getLogger('Worker')
        job_queue = '%s-jobs' % self._config.type
        self._queue = SQSConnection().get_queue(job_queue)
        self._queue.set_message_class(boto.sqs.message.MHMessage)
        if not self._queue:
            message = 'No job queue named %s' % job_queue
            self._logger.error(message)
            raise Exception(message)
        if revision:
            self._logger.warning('Starting worker revision %s' % revision)
        else:
            self._logger.warning('Starting worker')
        self._running = True

    def shutdown(self):
        if not self._running:
            return
        self._running = False
        self._logger.warning('Shutting down worker')

    def run(self):
        if not self._running:
            return

        m = self._queue.read(
            visibility_timeout=7200,
            wait_time_seconds=20,
        )
        if not m:
            if self._config.max_idle and \
                    time.time() - self._idle_since > self._config.max_idle:
                self.shutdown()
            return
        job = Job.from_message(m)
        self._logger.warning('Starting job for changeset %s on branch %s'
            % (job.changeset, job.branch), extra={
                'changeset': job.changeset,
                'branch': job.branch,
            })
        self._logger.warning('Finished job for changeset %s on branch %s'
            % (job.changeset, job.branch), extra={
                'changeset': job.changeset,
                'branch': job.branch,
            })
        self._queue.delete_message(m)
        self._idle_since = time.time()
