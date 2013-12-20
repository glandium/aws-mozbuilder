# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import os
import subprocess
import sys
import time

logging.basicConfig()


# Stolen from mozilla-central/python/mozbuild/mozbuild/pythonutil.py
def iter_modules_in_path(*paths):
    paths = [os.path.abspath(os.path.normcase(p)) + os.sep
             for p in paths]
    for name, module in sys.modules.items():
        if not hasattr(module, '__file__'):
            continue

        path = module.__file__

        if path.endswith('.pyc'):
            path = path[:-1]
        path = os.path.abspath(os.path.normcase(path))

        if any(path.startswith(p) for p in paths):
            yield path


class HandledException(Exception):
    def __init__(self, exception):
        self._wrapped_exception = exception


class SelfUpdater(object):
    # Only check for updates if last update was more than an hour ago.
    UPDATE_CHECK_PERIOD = 3600

    def __init__(self):
        self._path = os.path.dirname(__file__) or '.'
        self._can_update = os.path.isdir(os.path.join(self._path, '.git'))
        self._logger = logging.getLogger('SelfUpdater')
        self._last_update = 0
        if not self._can_update:
            self._logger.warning('Not under git control. Cannot self-update.')

    def maybe_update(self):
        try:
            self._maybe_update()
        except HandledException:
            pass
        except:
            import traceback
            self._logger.error(traceback.format_exc())

    def _maybe_update(self):
        if not self._can_update:
            return
        now = time.time()
        if now - self._last_update < self.UPDATE_CHECK_PERIOD:
            return
        self._last_update = now
        out = self._execute_command(['git', 'status', '--porcelain'])
        if any(not l.startswith('??') for l in out.splitlines()):
            self._logger.error('There are local changes to the server. '
                'Cannot self-update.')
            return
        mtimes = self.get_modules_mtimes()
        out = self._execute_command(['git', 'fetch', '--no-tags'])
        # git fetch outputs nothing when it fetches nothing
        if not out:
            return
        for line in out.splitlines():
            self._logger.warning(line)
        out = self._execute_command(['git', 'pull', '--ff-only'])
        for line in out.splitlines():
            self._logger.warning(line)
        new_mtimes = self.get_modules_mtimes()
        if mtimes == new_mtimes:
            self._logger.warning('No changes to the server. Not restarting.')
            return
        self._logger.warning('Server code changed. Restarting.')
        os.execl(sys.executable, sys.executable, __file__)

    def _execute_command(self, cmd):
        try:
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT,
                cwd=self._path)
            return out
        except subprocess.CalledProcessError as e:
            self._logger.error('Command "%s" failed with error code %d. '
                'Its output was:\n%s'
                % (' '.join(cmd), e.returncode, e.output))
            raise HandledException(e)

    def get_modules_mtimes(self):
        return dict(
            (p, os.path.getmtime(p))
            for p in iter_modules_in_path(self._path)
        )


def main():
    updater = SelfUpdater()

    while True:
        updater.maybe_update()
        time.sleep(1)


if __name__ == '__main__':
    main()
