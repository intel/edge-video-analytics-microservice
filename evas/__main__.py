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


"""EII VA Serving Service main entrypoint.
"""
import os
import cfgmgr.config_manager as cfg
from evas.log import get_logger
from distutils.util import strtobool
from evas.manager import EvasManager
from evas.log import configure_logging


if __name__ == '__main__':
    cfg_mgr = cfg.ConfigMgr()

    if 'DEV_MODE' in os.environ:
        dev_mode = strtobool(os.environ['DEV_MODE'])
    else:
        # By default, this will run NOT in dev mode
        dev_mode = False

    if 'PY_LOG_LEVEL' in os.environ:
        log_level = os.environ['PY_LOG_LEVEL'].upper()
    else:
        # Default log level is INFO
        log_level = 'INFO'

    # Configure EVAS logging globals
    configure_logging(log_level, dev_mode)

    # Get main logger
    log = get_logger(__name__)

    log.info('Initializing EVAS manager')
    manager = EvasManager(cfg_mgr, cfg_mgr.get_num_publishers(), cfg_mgr.get_num_subscribers())

    try:
        log.info('Running forever...')
        manager.run_forever()
    except Exception as e:
        log.exception('Error in EVAS manager', e)
        manager.stop()
