# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import socket
from util import (
    cached_property,
    Singleton,
)


class Config(Singleton):
    _slots = set(['instanceId', 'max_idle', 'region', 'type', 'branch',
        'after', 'mozconfig', 'patch', 'tooltool_manifest',
        'tooltool_base', 'pulse_user', 'pulse_password'])

    def __getattr__(self, name):
        if name not in Config._slots:
            raise AttributeError("'%s' object has no attribute '%s'"
                % (self.__class__.__name__, name))

        for category in ('identity', 'config_file', 'defaults'):
            dct = getattr(self, '_%s' % category)
            if name in dct:
                value = dct[name]
                if isinstance(value, (str, unicode)) and value.isdigit():
                    value = int(value)
                object.__setattr__(self, name, value)
                return value

    @cached_property
    def is_instance(self):
        try:
            socket.getaddrinfo('instance-data', 80)
            return True
        except socket.gaierror:
            return False

    @cached_property
    def _identity(self):
        try:
            if self.is_instance:
                import boto.utils
                return boto.utils.get_instance_identity()['document']
        except:
            pass
        return {'instanceId': 'unknown-%s' % socket.gethostname()}

    @cached_property
    def _config_file(self):
        file = os.path.join(os.path.dirname(__file__), 'config')
        try:
            userdata = open(file).read()
        except:
            userdata = ''
        config = {}
        for line in userdata.splitlines():
            if '=' in line:
                name, value = line.split('=', 1)
                config[name] = value
        return config

    @property
    def _defaults(self):
        return { 'max_idle': 1800 }
