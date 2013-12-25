# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto.sqs
import boto.sqs.connection
import boto.ec2
import boto.ec2.connection
import time
from config import Config
from util import Singleton


class EC2Connection(Singleton, boto.ec2.connection.EC2Connection):
    def __init__(self):
        # Config itself uses an EC2Connection, but we rely on the fact that
        # Config.region doesn't use it, and that the EC2Connection is not going
        # to be used to retrieve config.region.
        config = Config()
        region = boto.ec2.get_region(config.region)
        if not region:
            raise Exception('Unknown region: %s' % config.region)
        boto.ec2.connection.EC2Connection.__init__(self, region=region)


class SQSConnection(Singleton, boto.sqs.connection.SQSConnection):
    def __init__(self):
        config = Config()
        # boto.sqs doesn't have get_region() like boto.ec2.
        for region in boto.sqs.regions():
            if region.name == config.region:
                boto.sqs.connection.SQSConnection.__init__(self, region=region)
                return
        raise Exception('Unknown region: %s' % config.region)
