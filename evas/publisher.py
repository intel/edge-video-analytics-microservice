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

"""EII Pipeline Server results publisher.
"""
import gi

gi.require_version('Gst', '1.0')
# pylint: disable=wrong-import-position
import json
import queue
import string
import random
import cv2
import threading as th
import numpy as np
from gi.repository import Gst
from gstgva.util import gst_buffer_data
from server.gstreamer_app_source import GvaFrameData
import eii.msgbus as emb
from evas.log import get_logger


class EvasPublisher:
    """EII Pipeline Server publisher thread.
    """
    def __init__(self, app_cfg, pub_cfg, queue, publish_frame=True):
        """Constructor

        .. note:: This method immediately starts the publishing thread.

        :param json app_cfg: Application config
        :param cfg.Publisher pub_config: ConfigManager publisher configuration
        :param queue.Queue queue: Python queue of data to publish
        :param bool publish_frame: Flag for whether to publish the frame with
            the meta-data for the frame (df: True)
        """
        topics = pub_cfg.get_topics()
        assert len(topics) > 0, f'No specified topics'

        self.topic = topics[0]
        self.log = get_logger(f'{__name__} ({self.topic})')

        self.log.info(f'Initializing publisher for topic {self.topic}')
        self.msgbus_ctx = emb.MsgbusContext(pub_cfg.get_msgbus_config())
        self.publisher = self.msgbus_ctx.new_publisher(self.topic)

        self.app_cfg = app_cfg
        self.pub_cfg = pub_cfg
        self.queue = queue
        self.publish_frame = publish_frame
        self.stop_ev = th.Event()

        if self.publish_frame:
            self.log.info('Publishing frame with meta-data')
        else:
            self.log.info('Only publishing meta-data')

    def start(self):
        """Start the publisher.
        """
        self.log.debug('Starting publisher thread')
        self.th = th.Thread(target=self._run)
        self.th.start()

    def stop(self):
        """Stop the publisher.
        """
        if self.stop_ev.is_set():
            return
        self.stop_ev.set()
        self.th.join()
        self.th = None

    def _generate_image_handle(self, n):
        """Helper method to generate random alpha-numeric string

        :param n: random string length
        :type: int
        :return: Return random string
        :rtype: str
        """
        res = ''.join(random.choices(string.ascii_uppercase +
                                     string.digits, k = n))
        return res

    def _enable_encoding(self):
        """Method to check if encoding is enabled

        :return: Return whether encoding is enabled
        :rtype: bool
        """
        if 'encoding' in self.app_cfg.keys():
            encode_level = self.app_cfg['encoding']['level']
            encode_type = self.app_cfg['encoding']['type']
            if encode_type == 'JPEG'.lower():
                if encode_level >= 0 and encode_level <= 100:
                    return True
                else:
                    self.log.error("Invalid jpeg compression level")
            elif encode_type == 'PNG'.lower():
                if encode_level >= 0 and encode_level <= 9:
                    return True
                else:
                    self.log.error("Invalid png compression level")
            else:
                self.log.error("Invalid encoding type")

    def _encode_frame(self, frame, height, width):
        """Helper method to encode given frame

        :param frame: input frame
        :type: bytes
        :param height: height of the input frame
        :type: int
        :param width: width of the input frame
        :type: int
        :return: Return encoded frame
        :rtype: tuple where the first item is a bool and second item is numpy frame
        """
        enc_img = None
        enc_type = self.app_cfg['encoding']['type'].lower()
        enc_level = self.app_cfg['encoding']['level']
        data = np.frombuffer(frame, dtype="uint8")
        data = data.reshape((height, width, 3))
        if enc_type == 'jpeg':
            encode_param = [cv2.IMWRITE_JPEG_QUALITY, enc_level]
            enc_img = cv2.imencode('.jpg', data, encode_param)
        if enc_type == 'png':
            encode_param = [cv2.IMWRITE_PNG_QUALITY, enc_level]
            enc_img = cv2.imencode('.png', data, encode_param)

        return enc_img


    def _run(self):
        """Private thread run method.
        """
        self.log.debug('Publisher thread started')

        try:
            while not self.stop_ev.is_set():
                # TODO: Convert to a dictionary & blob and publish to msgbus
                try:
                    results = self.queue.get(timeout=0.5)
                    if not results:
                        continue

                    with gst_buffer_data(
                            results.sample.get_buffer(),
                            Gst.MapFlags.READ) as data:
                        frame = bytes(data)
                    caps = results.sample.get_caps()

                    gst_struct = caps.get_structure(0)
                    success, width = gst_struct.get_int('width')
                    if not success:
                        self.log.error('Failed to get buffer width')
                        continue
                    success, height = gst_struct.get_int('height')
                    if not success:
                        self.log.error('Failed to get buffer height')
                        continue

                    meta_data = {
                        'height': height,
                        'width': width,
                        'channels': 3,  # NOTE: This should not be constant
                        'caps': caps.to_string()
                    }

                    if 'img_handle' not in meta_data.keys():
                        meta_data['img_handle'] = self._generate_image_handle(10)

                    if results.video_frame:
                        gva_meta = []
                        regions = list(results.video_frame.regions())
                        messages = list(results.video_frame.messages())

                        # Add all messages
                        for msg in messages:
                            msg = json.loads(msg)
                            meta_data.update(msg)

                        for region in regions:
                            rect = region.rect()
                            meta = {
                                'x': rect.x,
                                'y': rect.y,
                                'height': rect.h,
                                'width': rect.w,
                                'object_id': region.object_id(),
                            }

                            tensors = list(region.tensors())
                            tensor_meta = []
                            for tensor in tensors:
                                tmeta = {
                                    'name': tensor.name(),
                                    'confidence': tensor.confidence(),
                                    'label_id': tensor.label_id()
                                }

                                if not tensor.is_detection():
                                    tmeta['label'] = tensor.label()

                                tensor_meta.append(tmeta)

                            meta['tensor'] = tensor_meta
                            gva_meta.append(meta)

                        meta_data['gva_meta'] = gva_meta

                    if (self.app_cfg.get('encoding', False)):
                        if self._enable_encoding() == True:
                            frame = self._encode_frame(frame, height, width)
                            meta_data['encoding_type'] = self.app_cfg['encoding']['type']
                            meta_data['encoding_level'] = self.app_cfg['encoding']['level']
                        else:
                            self.log.debug("Invalid encoding parameters")

                        frame = frame[1].tobytes()
                    else:
                        self.log.debug("Encoding not enabled")

                    self.log.info(f'Publishing message: {meta_data}')

                    msg = meta_data
                    if self.publish_frame:
                        msg = (msg, frame,)

                    self.publisher.publish(msg)
                except queue.Empty:
                    continue
        except Exception as e:
            # TODO: Check for more specific errors, attempt reconnect?
            self.log.exception(f'Error in publisher thread: {e}')
