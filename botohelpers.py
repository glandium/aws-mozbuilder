# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import boto.sqs
import boto.sqs.connection
import boto.s3.connection
import time
from config import Config
from util import Singleton


class SQSConnection(Singleton, boto.sqs.connection.SQSConnection):
    def __init__(self):
        config = Config()
        # boto.sqs doesn't have get_region() like boto.ec2.
        for region in boto.sqs.regions():
            if region.name == config.region:
                boto.sqs.connection.SQSConnection.__init__(self, region=region)
                return
        raise Exception('Unknown region: %s' % config.region)


class S3Connection(Singleton, boto.s3.connection.S3Connection):
    pass
