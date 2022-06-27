# Copyright (C) 2022 Intel Corporation
# SPDX-License-Identifier: MIT

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""EII Pipeline Server subscriber.
"""
from evas.log import get_logger
import eii.msgbus as emb
from server.gstreamer_app_source import GvaFrameData
from gstgva.util import gst_buffer_data
from gi.repository import Gst
import threading as th
import time
import queue
import json
import gi

gi.require_version('Gst', '1.0')
# pylint: disable=wrong-import-position


class EvasSubscriber:
    """EII Pipeline Server subscriber thread.
    """

    def __init__(self, sub_cfg, queue):
        """Constructor

        .. note:: This method immediately starts the subscriber thread.

        :param cfg.Subscriber sub_config: ConfigManager subscriber
                                          configuration
        :param queue.Queue queue: Python queue of data to publish
        """
        topics = sub_cfg.get_topics()
        assert len(topics) > 0, f'No specified topics'

        self.topic = topics[0]
        self.log = get_logger(f'{__name__} ({self.topic})')

        self.log.info(f'Initializing subscriber for topic {self.topic}')
        config = sub_cfg.get_msgbus_config()
        self.log.info("config : {}".format(config))
        self.msgbus_ctx = emb.MsgbusContext(config)
        self.subscriber = self.msgbus_ctx.new_subscriber(self.topic)

        self.sub_cfg = sub_cfg
        self.queue = queue
        self.stop_ev = th.Event()

    def start(self):
        """Start the subscriber.
        """
        self.log.debug('Starting subscriber thread')
        self.th = th.Thread(target=self._run)
        self.th.start()

    def stop(self):
        """Stop the subscriber.
        """
        if self.stop_ev.is_set():
            return
        self.stop_ev.set()
        self.th.join()
        self.th = None

    def _run(self):
        """Private thread run method.
        """
        self.log.debug('Subscriber thread started')

        try:
            while not self.stop_ev.is_set():
                try:
                    msg = self.subscriber.recv()
                    meta_data, blob = msg
                    self.log.info("Received message : {}".format(meta_data))

                    # Creating GstSample from raw bytes blob
                    bufferLength = len(blob)

                    # Allocate GstBuffer
                    buf = Gst.Buffer.new_allocate(None, bufferLength, None)
                    buf.fill(0, blob)

                    # Create GstSample from GstBuffer
                    gva_blob = Gst.Sample(buf, None, None, None)

                    self.queue.put(gva_blob)
                except queue.Empty:
                    continue
        except Exception as e:
            self.log.exception(f'Error in subscriber thread: {e}')
