# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto.sqs.message
from util  import cached_property
from worker import Worker


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


class BuilderWorker(Worker):
    @cached_property
    def _queue_name(self):
        return '%s-jobs' % self._config.type

    def _handle_message(self, msg):
        job = Job.from_message(msg)
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