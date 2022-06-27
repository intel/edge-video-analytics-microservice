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

# Specify all Docker arguments for the Dockerfile

ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS builder
LABEL description="Edge Video Analytics Microservice"

USER root

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /home/pipeline-server
RUN apt-get update && apt-get install -y --no-install-recommends git

ARG EII_VERSION
RUN git clone https://github.com/open-edge-insights/eii-core.git \
    --branch v${EII_VERSION} --single-branch

# Build the runtime image
FROM ${BASE_IMAGE} AS runtime

USER root

WORKDIR /home/pipeline-server

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y libcjson-dev

COPY ./run.sh /home/pipeline-server
COPY ./evas/ /home/pipeline-server/evas
RUN chmod a+x run.sh
COPY --from=builder /home/pipeline-server/eii-core/common/util/*.py util/

ARG PKG_SRC
ARG EII_VERSION
RUN wget ${PKG_SRC}/eii-utils-${EII_VERSION}.0-Linux.deb && \
    wget ${PKG_SRC}/eii-messagebus-${EII_VERSION}.0-Linux.deb && \
    wget ${PKG_SRC}/eii-configmanager-${EII_VERSION}.0-Linux.deb && \
    dpkg -i /home/pipeline-server/eii-utils-${EII_VERSION}.0-Linux.deb && \
    dpkg -i /home/pipeline-server/eii-messagebus-${EII_VERSION}.0-Linux.deb && \
    dpkg -i /home/pipeline-server/eii-configmanager-${EII_VERSION}.0-Linux.deb && \
    rm -rf eii-*.deb

RUN pip3 install --no-cache-dir eii-messagebus==${EII_VERSION} eii-configmanager==${EII_VERSION}

ARG EII_UID
ARG USER

RUN useradd -ms /bin/bash -G video,audio,users ${USER} -u $EII_UID && \
    chown ${USER}:${USER} -R /home/pipeline-server /root

ARG EII_SOCKET_DIR
RUN mkdir -p /home/${USER}/ && chown -R ${USER}:${USER} /home/${USER} && \
    mkdir -p ${EII_SOCKET_DIR} && chown -R ${USER}:${USER} $EII_SOCKET_DIR

ENV cl_cache_dir=/home/.cl_cache
RUN mkdir -p -m g+s $cl_cache_dir && chown ${USER}:users $cl_cache_dir
ENV XDG_RUNTIME_DIR=/home/.xdg_runtime_dir
RUN mkdir -p -m g+s $XDG_RUNTIME_DIR && chown ${USER}:users $XDG_RUNTIME_DIR
ENV LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:/usr/local/lib:/home/pipeline-server
USER $USER

ENTRYPOINT ["./run.sh"]