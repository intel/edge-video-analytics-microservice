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
ARG CMAKE_BUILD_TYPE="Release"
ARG RUN_TESTS="OFF"

FROM ${BASE_IMAGE} AS builder

LABEL description="Edge Video Analytics Microservice"
ARG EII_USER_NAME
USER root
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    make=4.2.1-1.2 \
    wget=1.20.3-1ubuntu2 \
    git && \
    rm -rf /var/lib/apt/lists/*

# Install VA Serving
ARG VA_SERVING_VERSION
RUN git clone https://github.com/intel/video-analytics-serving.git \
    --branch ${VA_SERVING_VERSION} \
    --single-branch /app && rm -rf .git

ARG EII_VERSION

RUN git clone https://github.com/open-edge-insights/eii-c-utils.git \
    --branch ${EII_VERSION} --single-branch 

RUN git clone https://github.com/open-edge-insights/eii-core.git \
    --branch ${EII_VERSION} --single-branch 

RUN git clone https://github.com/open-edge-insights/eii-messagebus.git \
    --branch ${EII_VERSION} --single-branch

ARG CMAKE_INSTALL_PREFIX="/usr/local"

ARG EIIMessageBus="./eii-messagebus"
ARG ConfigMgr="./eii-core/common/libs/ConfigMgr"
ARG EIICutils="./eii-c-utils"

RUN wget -O- https://cmake.org/files/v3.15/cmake-3.15.0-Linux-x86_64.tar.gz | \
        tar --strip-components=1 -xz -C /usr/local

RUN cd ${ConfigMgr} && rm -rf deps && ./install.sh
ARG INSTALL_PATH=${CMAKE_INSTALL_PREFIX}/lib

RUN cd ${EIIMessageBus} && rm -rf deps  && ./install.sh --cython

RUN cd ${EIICutils} && \
   ./install.sh && \
   rm -rf build && \
   mkdir build && \
   cd build && \
   cmake -DCMAKE_INSTALL_INCLUDEDIR=${CMAKE_INSTALL_PREFIX}/include -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DWITH_TESTS=${RUN_TESTS} -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE .. && \
   make && \
   if [ "${RUN_TESTS}" = "ON" ] ; then cd ./tests  && \
   ./config-tests  \
   ./log-tests \
   ./thp-tests \
   ./tsp-tests \
   ./thexec-tests \
   cd .. ; fi  && \
   make install

RUN pip3 install --user -r ${EIIMessageBus}/python/requirements.txt

RUN cd ${EIIMessageBus} && \
    rm -rf build/ && \
    mkdir build/ && \
    cd build/ && \
    cmake  -DWITH_TESTS=${RUN_TESTS} -DWITH_TESTS=${RUN_TESTS} -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} .. && \
    make install

RUN cd ${EIIMessageBus}/python && python3 setup.py install --user

RUN cd ${ConfigMgr} && \
   # Installing grpc from DEB package
   apt install ./grpc-1.29.0-Linux.deb && \
   rm -rf build && \
   mkdir build && \
   cd build && \
   cmake  -DWITH_TESTS=${RUN_TESTS} -DCMAKE_BUILD_TYPE=$CMAKE_BUILD_TYPE .. && \
   make install

RUN cd ${ConfigMgr}/python && \
   python3 setup.py.in install --user

# Runtime image
FROM ${BASE_IMAGE} AS runtime

USER root

WORKDIR /app

ARG EII_USER_NAME

RUN mkdir -p .local/lib
COPY --from=builder /app/requirements.service.txt .
COPY --from=builder /app/requirements.txt .
COPY --from=builder /app/vaserving /app/vaserving
COPY --from=builder /app/eii-core/common /app/common
COPY --from=builder /root/.local/lib .local/lib
COPY --from=builder /usr/local/lib/lib* /usr/local/lib/

ARG EII_UID

RUN useradd -r -u ${EII_UID} -G users,video,audio ${EII_USER_NAME}

ENV LD_LIBRARY_PATH ${LD_LIBRARY_PATH}:${CMAKE_INSTALL_PREFIX}/lib:${CMAKE_INSTALL_PREFIX}/lib/udfs
ENV LD_LIBRARY_PATH=/opt/intel/openvino/data_processing/dl_streamer/lib:/usr/local/lib/udfs:/usr/local/lib:$LD_LIBRARY_PATH
ENV CPLUS_INCLUDE_PATH=/opt/intel/openvino/data_processing/dl_streamer/include/gst/videoanalytics:/opt/intel/openvino/data_processing/dl_streamer/include/gst/videoanalytics/metadata
ENV PYTHONPATH=/usr/local/lib/:/EII:/app/common/libs/UDFLoader/build/:/app/common/video/udfs/python:/app/common/:${CMAKE_INSTALL_PREFIX}/lib
ENV PYTHONPATH ${PYTHONPATH}:/app/common/video/udfs/python:/app/common/:/app:/app/.local/lib/python3.8/site-packages:/root/.local/lib/python3.8/site-packages/:/app/common

# Copy over service code and install dependencies
COPY ./run.sh /app

COPY ./evas/ /app/evas

RUN pip3 install  --no-cache-dir -r /app/requirements.txt && \
    pip3 install  --no-cache-dir -r /app/requirements.service.txt && \
    rm -rf /app/requirements*.txt

RUN pip3 install markupsafe==2.0.1
ARG EII_SOCKET_DIR
RUN mkdir -p /home/${EII_USER_NAME}/ && chown -R ${EII_USER_NAME}:${EII_USER_NAME} /home/${EII_USER_NAME} && \
    mkdir -p ${EII_SOCKET_DIR} && chown -R ${EII_USER_NAME}:${EII_USER_NAME} $EII_SOCKET_DIR 
RUN chown ${EII_USER_NAME}:${EII_USER_NAME} /app /var/tmp
RUN chown -R ${EII_UID} .local/lib
ENV XDG_RUNTIME_DIR=/home/.xdg_runtime_dir
RUN mkdir -p -m g+s $XDG_RUNTIME_DIR && chown ${USER}:users $XDG_RUNTIME_DIR
RUN usermod -a -G users ${EII_USER_NAME}
ENV PYTHONPATH ${PYTHONPATH}:.local/lib

USER $EII_USER_NAME
ENTRYPOINT ["./run.sh"] 