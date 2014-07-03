# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import json
import re
import time
import socket
import threading
from contextlib import closing
from kombu import Exchange
from mozillapulse import consumers
from urllib2 import urlopen
from Queue import Queue, Empty


class PulseListener(object):
    instance = None

    def __init__(self):
        self.shutting_down = False

        # Let's generate a unique label for the script
        try:
            import uuid
            self.applabel = str(uuid.uuid4())
        except:
            from datetime import datetime
            self.applabel = str(datetime.now())


        self.queue = Queue()
        self.listener_thread = threading.Thread(target=self.pulse_listener)
        self.listener_thread.start()

    def pulse_listener(self):
        def got_message(data, message):
            message.ack()

            # Sanity checks
            payload = data.get('payload')
            if not payload:
                return

            change = payload.get('change')
            if not change:
                return

            revlink = change.get('revlink')
            if not revlink:
                return

            branch = change.get('branch')
            if not branch:
                return

            rev = change.get('rev')
            if not rev:
                return

            try:
                properties = { a: b for a, b, c in
                    change.get('properties', []) }
            except:
                properties = {}

            change['files'] = ['...']
            try:
                if 'polled_moz_revision' in properties or \
                        'polled_comm_revision' in properties or \
                        'releng' not in data.get('_meta', {}) \
                            .get('master_name', ''):
                    return
            except:
                pass

            self.queue.put((rev, branch, revlink, data, time.time()))

        while not self.shutting_down:
            # Connect to pulse
            pulse = consumers.BuildConsumer(applabel=self.applabel)

            # Tell pulse that you want to listen for all messages ('#' is
            # everything) and give a function to call every time there is a
            # message
            pulse.configure(topic=['change.#'], callback=got_message)

            # Manually do the work of pulse.listen() so as to be able to cleanly
            # get out of it if necessary.
            exchange = Exchange(pulse.exchange, type='topic')
            queue = pulse._create_queue(pulse.applabel, exchange,
                pulse.topic[0])
            consumer = pulse.connection.Consumer(queue,
                callbacks=[pulse.callback])
            with consumer:
                while not self.shutting_down:
                    try:
                        pulse.connection.drain_events(timeout=1)
                    except socket.timeout:
                        pass
                    except Exception as e:
                        # If we failed for some other reason than the timeout,
                        # cleanup and create a new connection.
                        break

            pulse.disconnect()


    def shutdown(self):
        if not self.shutting_down:
            self.shutting_down = True
            self.listener_thread.join()


class Pushlog(object):
    def __init__(self, branch, after=None, to=None):
        self._branch = branch
        self._after = after
        self._to = to

    def __iter__(self):
        pulse = None
        received = time.time()
        try:
            while True:
                if self._after:
                    pushlog = \
                        'https://hg.mozilla.org/%s/json-pushes?fromchange=%s' \
                        % (self._branch, self._after)
                    if self._to:
                        pushlog += '&tochange=%s' % self._to
                else:
                    pushlog = \
                        'https://hg.mozilla.org/%s/json-pushes' % self._branch

                with closing(urlopen(pushlog)) as fh:
                    pushes = json.loads(fh.read())

                if pushes:
                    for id, push in sorted(pushes.items(),
                            key=self.push_items_key):
                        if self._after:
                            push['received'] = received
                            yield push
                    self._after = push['changesets'][-1]

                    if self._to and self._after.startswith(self._to):
                        break

                if not pulse:
                    pulse = PulseListener()
                while True:
                    try:
                        rev, branch, revlink, data, received = \
                            pulse.queue.get(timeout=1)
                    except Empty:
                        if not pulse.listener_thread.is_alive():
                            pulse.shutdown()
                            break
                        continue
                    if branch == self._branch:
                        break

                if pulse and pulse.shutting_down:
                    break
        finally:
            if pulse:
                pulse.shutdown()

    @staticmethod
    def push_items_key(item):
        id, push = item
        return (push['date'], id)
