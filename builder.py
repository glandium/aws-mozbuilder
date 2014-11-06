# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import gzip
import hashlib
import os
import subprocess
import time
from botohelpers import S3Connection
from contextlib import closing
from pushlog import Pushlog
from StringIO import StringIO
from urllib2 import urlopen
from util  import cached_property
from worker import Worker


BUILD_AREA = '/srv/build'
# TODO: Use schroot sessions
WRAPPER_COMMAND = ['schroot', '-c', 'centos', '--']
HG_BASE = 'http://hg.mozilla.org/'


class BuilderWorker(Worker):
    @cached_property
    def _branch(self):
        return self._config.branch

    @cached_property
    def _queue(self):
        if self._config.pulse_user and self._config.pulse_password:
            pulse = (self._config.pulse_user, self._config.pulse_password)
        else:
            pulse = False
        return iter(Pushlog({ self._config.branch: self._config.after },
            pulse=pulse))

    @cached_property
    def _queue_name(self):
        return self._config.branch

    def shutdown(self):
        if not self._running:
            return
        Worker.shutdown(self)

    def run(self):
        if not self._running:
            return

        try:
            push = self._queue.next()
        except StopIteration:
            self.shutdown()
            return

        changeset = push['changesets'][-1]

        buildlog = BuildLog()
        status = 'failed'
        url = ''

        mozconfig = self._config.mozconfig
        if self._config.mozconfig:
            if self._config.mozconfig.startswith('http:') or \
                    self._config.mozconfig.startswith('https:'):
                mozconfig = ''
                try:
                    with closing(urlopen(self._config.mozconfig)) as fh:
                        mozconfig = fh.read()
                except:
                    # TODO: Log some failure cases.
                    pass
            else:
                mozconfig = '. $topsrcdir/%s\n' % self._config.mozconfig
        else:
            mozconfig = '. $topsrcdir/browser/config/mozconfigs/linux64/nightly\n'

        patch_url = self._config.patch
        patch=''
        if patch_url:
            try:
                with closing(urlopen(patch_url)) as fh:
                    patch = fh.read()
            except:
                # TODO: Log some failure cases.
                pass

        builder = Builder(buildlog, mozconfig, patch,
            self._config.tooltool_manifest, self._config.tooltool_base)
        for clobber in (False, True):
            buildlog.clear()
            started = time.time()
            self._logger.warning(
                'Starting job for changeset %s on branch %s (wait: %d + %d)'
                % (changeset, self._branch, int(push['received'] - push['date']),
                   int(started - push['received'])), extra={
                    'event': 'start',
                    'changeset': changeset,
                    'branch': self._branch,
                    'clobber': clobber,
                    'pushed': push['date'],
                    'received': push['received'],
                })
            try:
                builder.build(
                    branch=self._branch,
                    changeset=changeset,
                    clobber=clobber,
                )
                status = 'success'
            except BuildError:
                pass
            finished = time.time()
            try:
                url = self.store_log(buildlog)
            except:
                pass
            self._logger.warning('Finished job for changeset %s on branch %s (%s)'
                % (changeset, self._branch, status), extra={
                    'event': 'end',
                    'changeset': changeset,
                    'branch': self._branch,
                    'status': status,
                    'buildlog': url,
                    'clobber': clobber,
                    'clobbered': builder.clobbered,
                    'pushed': push['date'],
                    'received': push['received'],
                    'started': started,
                    'finished': finished,
                })
            if status == 'success':
                break

    @cached_property
    def _log_storage(self):
        return S3Connection().get_bucket(self._config.type, validate=False)

    def store_log(self, log):
        data = StringIO()
        hash = hashlib.sha1()
        with gzip.GzipFile(mode='w', compresslevel=9, fileobj=data) as fh:
            log.serialize(HashProxy(fh, hash))
        hash = hash.hexdigest()
        path = 'logs/%s/%s/%s.gz' % (hash[0], hash[1], hash)
        key = self._log_storage.new_key(path)
        key.set_contents_from_string(data.getvalue(), headers={
            'x-amz-acl': 'public-read',
            'Content-Type': 'text/plain',
            'Content-Encoding': 'gzip',
            'Cache-Control': 'max-age=1296000', # Two weeks
        })

        data.close()
        return path


class HashProxy(object):
    def __init__(self, fh, hash):
        self._fh = fh
        self._hash = hash

    def write(self, s):
        self._fh.write(s)
        self._hash.update(s)


class BuildError(RuntimeError):
    pass


class Builder(object):
    def __init__(self, buildlog, mozconfig, patch, tooltool_manifest,
            tooltool_base):
        self._log = buildlog
        self._mozconfig = mozconfig
        self._patch = patch
        self._tooltool = (tooltool_manifest, tooltool_base) \
            if tooltool_manifest and tooltool_base else None
        self.clobbered = False

    def execute(self, command, input=None, cwd=None, wrapper=WRAPPER_COMMAND):
        start = time.time()
        proc = subprocess.Popen(wrapper + command,
            stdin=subprocess.PIPE if input else None,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=cwd)
        stdout, stderr = proc.communicate(input)
        assert not stderr
        end = time.time()
        self._log.add(
            command=command,
            output=stdout,
            duration=end - start,
            status=proc.returncode,
        )
        if proc.returncode:
            raise BuildError("Command %s failed" % command)

    def prepare_source(self, branch, changeset, clobber=False):
        source_dir = os.path.join(BUILD_AREA, os.path.basename(branch))
        clone = not os.path.exists(source_dir)
        if clone:
            clone_branch = 'mozilla-central' if branch == 'try' else branch
            self.execute(['hg', 'clone', '--noupdate',
                          HG_BASE + clone_branch, source_dir])
        hg = ['hg', '-R', source_dir]
        if not clone:
            self.execute(hg + ['id', '-i'])
        if not clone or branch == 'try':
            self.execute(hg + ['pull', HG_BASE + branch, '-r', changeset])
        self.execute(hg + ['update', '-C', '-r', changeset])
        purge_cmd = hg + ['--config', 'extensions.purge=', 'purge']
        if clobber:
            purge_cmd.append('--all')
        self.execute(purge_cmd)
        if self._patch:
            self.execute(['patch', '-d', source_dir, '-p1'], self._patch,
                wrapper=[])
        if self._tooltool:
            tooltool_path = os.path.join(os.path.dirname(__file__), 'tooltool',
                'tooltool.py')
            manifest_path = os.path.join(source_dir, self._tooltool[0])
            self.execute(['cat', manifest_path])
            self.execute(['python', tooltool_path, '--url', self._tooltool[1],
                '-m', manifest_path, '--overwrite',
                '-c', os.path.join(BUILD_AREA, 'tooltool'),
                'fetch'], cwd=source_dir, wrapper=[])
            if os.path.exists(os.path.join(source_dir, 'setup.sh')):
                self.execute(['bash', '-xe', 'setup.sh'], cwd=source_dir,
                    wrapper=[])
        return source_dir

    def build(self, branch, changeset, clobber=False):
        # Add some entropy to the log
        self.execute(['date'])
        self.execute(
            ['env', 'CCACHE_DIR=/srv/cache', 'ccache', '-z', '-M', '10G'])
        source_dir = self.prepare_source(branch, changeset, clobber=clobber)
        obj_dir = os.path.join(BUILD_AREA, 'obj-' + os.path.basename(branch))
        mozconfig = os.path.join(source_dir, '.mozconfig')
        with open(mozconfig, 'w') as fh:
            if self._mozconfig:
                fh.write(self._mozconfig)
            fh.write('mk_add_options MOZ_OBJDIR=%s\n' % obj_dir)
        self.execute(['cat', mozconfig])
        if clobber:
            self.execute(['rm', '-rf', obj_dir])
        self.clobbered = clobber or self.will_clobber(obj_dir, source_dir)
        try:
            self.execute(['env', 'CCACHE_DIR=/srv/cache', 'make', '-f',
                'client.mk', '-C', source_dir])
        finally:
            self.execute(['env', 'CCACHE_DIR=/srv/cache', 'ccache', '-s'])

    def will_clobber(self, obj_dir, src_dir):
        """Returns a bool indicating whether a tree clobber is going to be performed."""

        obj_clobber = os.path.join(obj_dir, 'CLOBBER')
        # No object directory clobber file means we're good.
        if not os.path.exists(obj_clobber):
            return False

        src_clobber = os.path.join(src_dir, 'CLOBBER')
        # Object directory clobber older than current is fine.
        if os.path.getmtime(src_clobber) <= \
            os.path.getmtime(obj_clobber):
            return False

        return True


class BuildLog(object):
    def __init__(self):
        self.clear()

    def add(self, **kwargs):
        assert set(kwargs.keys()) == \
            set(['command', 'output', 'duration', 'status'])
        self._data.append(kwargs)

    def clear(self):
        self._data = []

    def _serialize_one(self, item):
        command, output, duration, status = \
            item['command'], item['output'], item['duration'], item['status']

        return ''.join([
            '===== Started %s\n' % command,
            output,
            '===== %s %s in %d:%02d\n' % (
                'Failed (status: %d)' % (status) if status else 'Finished',
                command,
                duration / 60,
                duration % 60,
            ),
        ])

    def serialize(self, fh):
        for item in self._data:
            fh.write(self._serialize_one(item))
            fh.write('\n')
