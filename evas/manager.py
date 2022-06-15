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

"""EII VA Serving Service manager.
"""
import json
import queue
import threading
import cfgmgr.config_manager as cfg
from evas.publisher import EvasPublisher
from evas.subscriber import EvasSubscriber
from evas.log import get_logger, LOG_LEVEL
from vaserving.gstreamer_app_source import GvaFrameData
from vaserving.vaserving import VAServing


# Global location for where to save the UDFs configuration file (if needed)
CONFIG_LOC = '/tmp/config.json'


class EvasManager:
    """EII Video Analytiocs Serving service manager.

    This manager is responsible for managing the entire runtime setup and
    behaivior of the service. This includes managing the EII Message Bus
    publisher and subscriber, the EII Message Bus request/response server for
    handling commands, and the launch/stopping of pipeline in VA Serving.
    """
    def __init__(self, cfg_mgr, num_publishers, num_subscribers):
        """Constructor

        :param cfgmgr.config_manager.ConfigMgr cfg_mgr: EII config manager
        :param int num_publishers: Number of publishers
        :param int num_subscribers: Number of subscribers
        """
        global CONFIG_LOC

        self.log = get_logger(__name__)
        self.cfg = cfg_mgr

        self.log.debug('Getting EVAS app config')
        self.app_cfg = self.cfg.get_app_config().get_dict()
        self.log.debug(f'EVAS Serving Config: {self.app_cfg}')

        self.model_params = {}

        if 'udfs' in self.app_cfg:
            self.log.info(f'Config specified UDFs, saving to {CONFIG_LOC}')
            udfs = {
                'udfs': self.app_cfg['udfs']
            }
            with open(CONFIG_LOC, 'w') as f:
                json.dump(udfs, f)

            self.model_params['config'] = CONFIG_LOC

        self.output_queue, self.input_queue = [], []
        self.publisher, self.subscriber = [], []
        self.pipeline = []

        for i in range(num_publishers):
            thread = threading.Thread(target=self.start_va_serving_instance,
                                    args=(i,))
            thread.start()

    def start_va_serving_instance(self, index):
        """Start each instance of EVAS service
        """
        # Input and output queues
        self.output_queue[index] = queue.Queue()
        self.input_queue[index] = None

        if self.app_cfg['source'] == 'msgbus':
            self.input_queue[index] = queue.Queue()
            self.log.debug('Initializing EVAS subscriber')
            if self.cfg.get_num_subscribers != -1:
                sub_cfg = self.cfg.get_subscriber_by_index(0)
                self.log.debug('Getting EVAS subscriber configuration')
                self.subscriber[index] = EvasSubscriber(sub_cfg, self.input_queue[index])
                self.subscriber[index].start()
        elif self.app_cfg['source'] != 'gstreamer':
            raise RuntimeError(f'Unsupported source: {self.app_cfg["source"]}')
        else:
            self.subscriber[index] = None

        self.log.debug('Getting EVAS publisher configuration')
        pub_cfg = self.cfg.get_publisher_by_index(index)

        self.log.debug('Initializing EVAS publisher')
        pub_frame = self.app_cfg.get('publish_frame', False)
        self.publisher[index] = EvasPublisher(self.app_cfg, pub_cfg,
                                       self.output_queue[index], pub_frame)
        self.publisher[index].start()

        self.log.info('Starting VA Serving')
        VAServing.start({
            'log_level': LOG_LEVEL,
            'ignore_init_errors': True
        })

        # NOTE: Need to probably generalize this a bit more
        # Questions:
        # - What if I wanted to use a model, but switch out a URI source with
        #       say a GigE vision camera? New pipeline? Is there an easier way?
        if self.subscriber[index]:
            del self.app_cfg['source_parameters']['uri']
            self.app_cfg['source_parameters'] = {
                "type": "application",
                "class": "GStreamerAppSource",
                "input": self.input_queue[index],
            }
        src = self.app_cfg['source_parameters']
        self.log.info("App_cfg {}".format(self.app_cfg))
        dest = {
            'metadata': {
                'type': 'application',
                'class': 'GStreamerAppDestination',
                'output': self.output_queue[index],
                'mode': 'frames'  # TODO: Should this be something else? options?
            }
        }

        if 'model_parameters' in self.app_cfg:
            self.model_params.update(self.app_cfg['model_parameters'])
        pipeline = self.app_cfg['pipeline']
        pipeline_version = self.app_cfg['pipeline_version']

        self.log.info(
                f'Creating VA serving pipeline {pipeline}/{pipeline_version}')
        self.pipeline[index] = VAServing.pipeline(pipeline, pipeline_version)
        if self.pipeline[index] is None:
            raise RuntimeError('Failed to initialize VA Serving pipeline')

        self.log.info('Starting VA serving pipeline {} {}'.format(src, dest))
        self.pipeline[index].start(source=src,
                                   destination=dest,
                                   parameters=self.model_params)

    def stop(self):
        """Stop the EVAS service.
        """
        VAServing.stop()
        for publisher in self.publisher:
            publisher.stop()
        for subscriber in self.subscriber:
            if subscriber is not None:
                subscriber.stop()

    def run_forever(self):
        """Start the EVAS service and run forever.
        """
        self.log.debug('Running forever...')
        VAServing.wait()

    def _config_update_callback(self):
        """Private method for handling configuration changes reported by the
        configuration manager.
        """
        # TODO: Implement this method...
        pass
