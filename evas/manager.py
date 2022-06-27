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

"""EII Pipeline Server Service manager.
"""
import json
import queue
import cfgmgr.config_manager as cfg
from evas.publisher import EvasPublisher
from evas.subscriber import EvasSubscriber
from evas.log import get_logger, LOG_LEVEL
from server.gstreamer_app_source import GvaFrameData
from server.pipeline_server import PipelineServer


# Global location for where to save the UDFs configuration file (if needed)
CONFIG_LOC = '/tmp/config.json'


class EvasManager:
    """EII Pipeline server service manager.

    This manager is responsible for managing the entire runtime setup and
    behaivior of the service. This includes managing the EII Message Bus
    publisher and subscriber, the EII Message Bus request/response server for
    handling commands, and the launch/stopping of pipeline in Pipeline Server.
    """

    def __init__(self, cfg_mgr):
        """Constructor

        :param cfgmgr.config_manager.ConfigMgr cfg_mgr: EII config manager
        """
        global CONFIG_LOC

        self.log = get_logger(__name__)
        self.cfg = cfg_mgr

        self.log.debug('Getting EVAS app config')
        self.app_cfg = self.cfg.get_app_config().get_dict()
        self.log.debug(f'EVAS Serving Config: {self.app_cfg}')

        # Input and output queues
        self.output_queue = queue.Queue()
        self.input_queue = None

        model_params = {}

        if 'udfs' in self.app_cfg:
            self.log.info(f'Config specified UDFs, saving to {CONFIG_LOC}')
            udfs = {
                'udfs': self.app_cfg['udfs']
            }
            with open(CONFIG_LOC, 'w') as f:
                json.dump(udfs, f)

            model_params['config'] = CONFIG_LOC

        if self.app_cfg['source'] == 'msgbus':
            self.input_queue = queue.Queue()
            self.log.debug('Initializing EVAS subscriber')
            if self.cfg.get_num_subscribers != -1:
                sub_cfg = self.cfg.get_subscriber_by_index(0)
                self.log.debug('Getting EVAS subscriber configuration')
                self.subscriber = EvasSubscriber(sub_cfg, self.input_queue)
                self.subscriber.start()
        elif self.app_cfg['source'] != 'gstreamer':
            raise RuntimeError(f'Unsupported source: {self.app_cfg["source"]}')
        else:
            self.subscriber = None

        self.log.debug('Getting EVAS publisher configuration')
        pub_cfg = self.cfg.get_publisher_by_index(0)

        self.log.debug('Initializing EVAS publisher')
        pub_frame = self.app_cfg.get('publish_frame', False)
        self.publisher = EvasPublisher(self.app_cfg, pub_cfg,
                                       self.output_queue, pub_frame)
        self.publisher.start()

        self.log.info('Starting Pipeline Server')
        PipelineServer.start({
            'log_level': LOG_LEVEL,
            'ignore_init_errors': True
        })

        # NOTE: Need to probably generalize this a bit more
        # Questions:
        # - What if I wanted to use a model, but switch out a URI source with
        #       say a GigE vision camera? New pipeline? Is there an easier way?
        if self.subscriber:
            del self.app_cfg['source_parameters']['uri']
            self.app_cfg['source_parameters'] = {
                "type": "application",
                "class": "GStreamerAppSource",
                "input": self.input_queue,
            }
        src = self.app_cfg['source_parameters']
        self.log.info("App_cfg {}".format(self.app_cfg))
        dest = {
            'metadata': {
                'type': 'application',
                'class': 'GStreamerAppDestination',
                'output': self.output_queue,
                'mode': 'frames'  # TODO: Should this be something else? options?
            }
        }

        if 'model_parameters' in self.app_cfg:
            model_params.update(self.app_cfg['model_parameters'])
        pipeline = self.app_cfg['pipeline']
        pipeline_version = self.app_cfg['pipeline_version']

        self.log.info(
            f'Creating Pipeline Server pipeline {pipeline}/{pipeline_version}')
        self.pipeline = PipelineServer.pipeline(pipeline, pipeline_version)
        if self.pipeline is None:
            raise RuntimeError('Failed to initialize Pipeline Server pipeline')

        self.log.info('Starting Pipeline Server pipeline {} {}'.format(src, dest))
        self.pipeline.start(source=src,
                            destination=dest,
                            parameters=model_params)

    def stop(self):
        """Stop the EVAS service.
        """
        PipelineServer.stop()
        self.publisher.stop()
        if self.subscriber is not None:
            self.subscriber.stop()

    def run_forever(self):
        """Start the EVAS service and run forever.
        """
        self.log.debug('Running forever...')
        PipelineServer.wait()

    def _config_update_callback(self):
        """Private method for handling configuration changes reported by the
        configuration manager.
        """
        # TODO: Implement this method...
        pass
