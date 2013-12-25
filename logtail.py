# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import sys
from config import Config
from util import cached_property
from worker import Worker

class Formatter(logging.Formatter):
    def format(self, record):
        if hasattr(record, 'instanceId'):
            fmt = '(%(instanceId)s) %(levelname)s:%(name)s:%(message)s'
            if hasattr(record, 'buildlog') and record.buildlog:
                fmt += '\n   %(buildlog)s'
        else:
            fmt = '%(levelname)s:%(name)s:%(message)s'
        record.message = record.getMessage()
        return fmt % record.__dict__
        

handler = logging.StreamHandler()
handler.setFormatter(Formatter())
logging.getLogger('').addHandler(handler)


class LogTailWorker(Worker):
    MAPPING = {
        'level': 'levelname',
        'message': 'msg',
    }

    @cached_property
    def _queue_name(self):
        return '%s-logs' % self._config.type

    def _handle_message(self, msg):
        body = msg.get_body()
        record = {}

        for key, logging_key in self.MAPPING.items():
            record[logging_key] = body[key]

        for key, value in body.items():
            if key not in self.MAPPING:
                record[key] = body[key]

        record['levelno'] = getattr(logging, record['levelname'])
        rec = logging.makeLogRecord(record)
        self._logger.handle(rec)


def main(args):
    Config().max_idle = 0
    worker = LogTailWorker()
    while True:
        worker.run()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
