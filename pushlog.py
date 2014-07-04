# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import json
import re
import time
import socket
import threading
from collections import OrderedDict
from contextlib import closing
from kombu import Exchange
from mozillapulse import consumers
from urllib2 import urlopen
from Queue import Queue, Empty


class PulseListener(object):
    instance = None

    def __init__(self, filter_callback):
        self.shutting_down = False
        self._filter = filter_callback

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

            data = {
                'rev': rev,
                'branch': branch,
                'revlink': revlink,
                'data': data,
                'received': time.time(),
            }
            if self._filter(data):
                self.queue.put(data)

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

    def _iter(self, timeout, pending_only=False):
        while True:
            try:
                yield self.queue.get(timeout=timeout)
            except Empty as e:
                if not self.listener_thread.is_alive():
                    self.shutdown()
                if pending_only or self.shutting_down:
                    break
            except KeyboardInterrupt:
                self.shutdown()
                raise

    def __iter__(self):
        for d in self._iter(timeout=1):
            yield d

    def iter_pending(self):
        for d in self._iter(timeout=0, pending_only=True):
            yield d


class Pushlog(object):
    def __init__(self, branches, pulse=True):
        assert isinstance(branches, (list, dict))
        # Normalize branches.
        if isinstance(branches, list):
            self.branches = { b: None for b in branches }
        elif isinstance(branches, dict):
            self.branches = {}
            for b, v in branches.items():
                assert isinstance(v, (str, unicode)) or v is None
                self.branches[b] = v
        self._pulse = pulse

    def __iter__(self):
        if self._pulse:
            pulse = PulseListener(lambda data: data['branch'] in self.branches)
        else:
            class DummyPulse(object):
                def __iter__(self):
                    return iter([])

                def iter_pending(self):
                    return iter([])

            pulse = DummyPulse()

        try:
            for push in self._iter(pulse):
                yield push
        finally:
            pulse.shutdown()


    def _iter(self, pulse):
        pushes = {}
        for branch, after in self.branches.items():
            received = time.time()
            pushes[branch] = self.get_pushes(branch, fromchange=after)
            for push in pushes[branch].values():
                push['received'] = received

        for data in pulse.iter_pending():
            rev = data['rev']
            branch = data['branch']
            for rev, push in self.get_pushes(branch, changeset=rev).items():
                # In the unlikely event the changeset was received by pulse
                # while reading json from other branches, adjust its received
                # time.
                if rev in pushes[branch]:
                    push['received'] = data['received']
                else:
                    pushes[branch][rev] = push

        for push in sorted((p for b in pushes for p in pushes[b].values()),
                key=lambda p: p['date']):
            yield push
            self.branches[push['branch']] = push['changesets'][-1]

        for data in pulse:
            rev = data['rev']
            branch = data['branch']
            for push in self.get_pushes(branch, changeset=rev).values():
                push['received'] = data['received']
                yield push
                self.branches[branch] = rev

    @staticmethod
    def get_pushes(branch, **args):
        def push_items_key(item):
            id, push = item
            return (push['date'], id)

        url = 'https://hg.mozilla.org/%s/json-pushes?%s' % (
            branch,
            '&'.join('%s=%s' % (k, v) for k, v in args.items()),
        )
        for retry in range(0, 5):
            try:
                with closing(urlopen(url)) as fh:
                    result = OrderedDict()
                    pushes = sorted(json.loads(fh.read()).items(),
                        key=push_items_key)
                    for id, push in pushes:
                        push['branch'] = branch
                        result[push['changesets'][-1]] = push
                    return result
            except:
                time.sleep(1)
                continue
            break

        return {}
