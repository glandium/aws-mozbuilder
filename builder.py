# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import gzip
import hashlib
import os
import subprocess
import time
from boto.sqs.jsonmessage import JSONMessage
from botohelpers import S3Connection
from contextlib import closing
from StringIO import StringIO
from urllib2 import urlopen
from util  import cached_property
from worker import Worker


BUILD_AREA = '/srv/build'
# TODO: Use schroot sessions
WRAPPER_COMMAND = ['schroot', '-c', 'centos', '--']
HG_BASE = 'http://hg.mozilla.org/'


class Job(object):
    def __init__(self, branch, changeset):
        self.branch = branch
        self.changeset = changeset

    @staticmethod
    def from_message(msg):
        assert isinstance(msg, JSONMessage)
        return Job(msg['branch'], msg['changeset'])

    def to_message(self):
        msg = JSONMessage()
        msg['branch'] = self.branch
        msg['changeset'] = self.changeset
        return msg


class BuilderWorker(Worker):
    @cached_property
    def _queue_name(self):
        return '%s-jobs' % self._config.type

    def shutdown(self):
        if not self._running:
            return
        Worker.shutdown(self)
        if self._config.is_instance:
            from botohelpers import AutoScaleConnection
            conn = AutoScaleConnection()
            if conn.get_all_autoscaling_instances([self._config.instanceId]):
                conn.terminate_instance(self._config.instanceId,
                    decrement_capacity=True)
            else:
                from botohelpers import EC2Connection
                conn = EC2Connection()
                instances = conn.get_only_instances([self._config.instanceId])
                attr_name = 'instanceInitiatedShutdownBehavior'
                # There should really be only one
                for instance in instances:
                    attr = instance.get_attribute(attr_name)
                    if attr[attr_name] == 'terminate':
                        instance.terminate()
                    else:
                        instance.stop()

    def _handle_message(self, msg):
        job = Job.from_message(msg)
        self._logger.warning('Starting job for changeset %s on branch %s'
            % (job.changeset, job.branch), extra={
                'changeset': job.changeset,
                'branch': job.branch,
            })
        buildlog = BuildLog()
        status = 'failed'
        url = ''

        mozconfig_url = \
            'https://%s.s3.amazonaws.com/mozconfig' % self._config.type
        mozconfig=''
        try:
            with closing(urlopen(mozconfig_url)) as fh:
                mozconfig = fh.read()
        except:
            # TODO: Log some failure cases.
            pass

        patch_url = \
            'https://%s.s3.amazonaws.com/patch' % self._config.type
        patch=''
        try:
            with closing(urlopen(patch_url)) as fh:
                patch = fh.read()
        except:
            # TODO: Log some failure cases.
            pass

        builder = Builder(buildlog, mozconfig, patch)
        try:
            builder.build(
                branch=job.branch,
                changeset=job.changeset,
            )
            status = 'success'
        except BuildError:
            pass
        try:
            url = self.store_log(buildlog)
        except:
            pass
        self._logger.warning('Finished job for changeset %s on branch %s (%s)'
            % (job.changeset, job.branch, status), extra={
                'changeset': job.changeset,
                'branch': job.branch,
                'status': status,
                'buildlog': url,
            })

    @cached_property
    def _log_storage(self):
        return S3Connection().get_bucket(self._config.type)

    def store_log(self, log):
        data = StringIO()
        hash = hashlib.sha1()
        with gzip.GzipFile(mode='w', compresslevel=9, fileobj=data) as fh:
            log.serialize(HashProxy(fh, hash))
        hash = hash.hexdigest()
        path = 'logs/%s/%s/%s.txt.gz' % (hash[0], hash[1], hash)
        key = self._log_storage.new_key(path)
        key.set_contents_from_string(data.getvalue())
        key.set_acl('public-read')

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
    def __init__(self, buildlog, mozconfig, patch):
        self._log = buildlog
        self._mozconfig = mozconfig
        self._patch = patch

    def execute(self, command, input=None):
        start = time.time()
        proc = subprocess.Popen(WRAPPER_COMMAND + command,
            stdin=subprocess.PIPE if input else None,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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

    def prepare_source(self, branch, changeset):
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
        try:
            self.execute(hg + ['--config', 'extensions.mq=', 'strip',
                '--no-backup', 'not(:%s)' % changeset])
        except BuildError:
            pass
        self.execute(hg + ['--config', 'extensions.purge=', 'purge', '--all'])
        if self._patch:
            self.execute(['patch', '-d', source_dir, '-p1'], self._patch)
        return source_dir

    def build(self, branch, changeset):
        # Add some entropy to the log
        self.execute(['date'])
        source_dir = self.prepare_source(branch, changeset)
        obj_dir = os.path.join(BUILD_AREA, 'obj-' + os.path.basename(branch))
        mozconfig = os.path.join(source_dir, '.mozconfig')
        with open(mozconfig, 'w') as fh:
            fh.write('. $topsrcdir/browser/config/mozconfigs/linux64/nightly\n')
            if self._mozconfig:
                fh.write(self._mozconfig)
            fh.write('mk_add_options MOZ_OBJDIR=%s\n' % obj_dir)
        self.execute(['cat', mozconfig])
        self.execute(['make', '-f', 'client.mk', '-C', source_dir])


class BuildLog(object):
    def __init__(self):
        self._data = []

    def add(self, **kwargs):
        assert set(kwargs.keys()) == \
            set(['command', 'output', 'duration', 'status'])
        self._data.append(kwargs)

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
