# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import json
import logging
import sys
import time
from builder import Job
from config import Config
from contextlib import closing
from urllib2 import urlopen
from util import cached_property
from worker import Worker

logging.basicConfig()


class ReplayWorker(Worker):
    def __init__(self, branch, after, to):
        Worker.__init__(self)
        self._branch = branch
        pushlog = 'https://hg.mozilla.org/%s/json-pushes?fromchange=%s' \
            % (branch, after)
        if to != 'tip':
            pushlog += '&tochange=%s' % to

        with closing(urlopen(pushlog)) as fh:
            self._pushes = json.loads(fh.read())

    @cached_property
    def _queue_name(self):
        return '%s-jobs' % self._config.type

    def run(self):
        config = Config()
        refdate = None
        self._logger.warning('Replaying %d pushes from branch %s.'
            % (len(self._pushes), self._branch))
        for id, push in sorted(self._pushes.items(), key=self.push_items_key):
            if refdate:
                delta = push['date'] - refdate
                if delta > config.max_idle:
                    delta = config.max_idle
                if delta:
                    self._logger.warning('Waiting %d seconds until next push.'
                        % delta)
                    time.sleep(delta)
            refdate = push['date']
            job = Job(
                branch=self._branch,
                changeset=push['changesets'][-1],
            )
            self._logger.warning('Pushing changeset %s' % job.changeset)
            self._queue.write(job.to_message())

    @staticmethod
    def push_items_key(item):
        id, push = item
        return (push['date'], id)


def main(args):
    parser = argparse.ArgumentParser(description='Replay mercurial pushes')
    parser.add_argument('branch', default='try', nargs='?',
        help='Mercurial branch')
    parser.add_argument('--after', required=True,
        help='Changeset after which to start')
    parser.add_argument('--to', help='Last changeset', default='tip')

    opts = parser.parse_args(args)

    worker = ReplayWorker(opts.branch, opts.after, opts.to)
    worker.run()
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
