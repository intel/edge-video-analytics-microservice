Edge Video Analytics Microservice for EII
=========================================

The Edge Video Analytics Microservice combines the video ingestion and analytics
capabilities provided by EII visual ingestion and analytics modules.

## Fetching EII source code

Since the EII Edge Video Analytics Microservice depends on EII provisioning,
it has to be built in EII context by fetching the EII source code.
Please follow the steps mentioned below to fetch the EII source code

```sh
repo init -u "https://github.com/open-edge-insights/eii-manifests.git"

repo sync
```
For more details, refer [here](https://github.com/open-edge-insights/eii-manifests).

## Running

Since the model files are large in size, they are not part of the repo.
Download the required model files to be used for the pipeline mentioned in
[config](./config.json) by following the steps mentioned in [README](../README.md#running-the-image)

With the pre-requisite of provisioning the EII stack by referring to
[README.md](https://github.com/open-edge-insights/eii-core/blob/master/README.md#provision) done,
please follow the steps mentioned below to run the Edge Video Analytics Microservice in EII context

```sh
cd [WORK_DIR]/IEdgeInsights/build

# Execute the builder.py script
python3 builder.py -f usecases/evas.yml

# Create some necessary items for the service
sudo mkdir -p /opt/intel/eii/models/

# Copy the downloaded model files to /opt/intel/eii
sudo cp -r models /opt/intel/eii/

# Build the docker containers
docker-compose -f docker-compose-build.yml build

# Start the docker containers
docker-compose up -d ia_configmgr_agent # workaround for now, we are exploring on getting this and the sleep avoided
sleep 30 # If any failures like configmgr data store client certs or msgbus certs failures, please increase this time to a higher value
docker-compose up -d
```

This repository provides the Deep Learning Streamer(DL Streamer) pipelines needed
for using a URI source and sending the ingested frames using the EII MsgBus Publisher.
The commands above which create the, "models/", directory and the, "pipelines/",
install needed directories for the Edge Video Analytics Microservice to run the
DL Streamer pipelines.

## Configuration

Please see the [config](config.json) file for the configuration of the
Edge Video Analytics Microservice. The default configuration will start the
object_detection demo for EII.

The table below describes each of the configuration attributes currently
supported.

> **TODO:** Need to document these parameters much more.

|      Parameter      |                                                     Description                                                |
| :-----------------: | -------------------------------------------------------------------------------------------------------------- |
| `source`            | Source of the frames, must be `"gstreamer"` or `"msgbus"`.                                                    |
| `source_parameters` | The parameters for the source element. The provided object is the typical parameters.                          |
| `pipeline`          | The name of the DL Streamer pipeline to use (should correspond to a directory in the pipelines directory).      |
| `pipeline_version`  | The version of the pipeline to use. This typically is a subdirectory of a pipeline in the pipelines directory. |
| `publish_frame`     | Boolean flag for whether to publish the meta-data and the analyzed frame, or just the meta-data.               |
| `model_parameters`  | Provides the parameters for the model used for inference.                 |

### Configuring the Interfaces

The service only supports launching a single pipeline and publishing on a
single topic currently. This means, in the configuration file ("config.json"),
the single JSON object in the `Publisher` list is where the configuration
resides for the published data. See the EII documentation for more details on
the structure of this.

### Configuring EII subscriber and publisher

The Edge Video Analytics Microservice also supports subscribing and publishing messages/frames
using the EIIMessageBus.
The endpoint details for the EII service you need to subscribe from is to be
provided in the **Subscribers** section in [config](config.json) and the endpoints
where you need to publish to are to be provided in **Publishers** section in
[config](config.json).

For enabling injection of frames into the GStreamer pipeline obtained from
EIIMessageBus, please ensure the following changes are done:

* The source parameter in [config](config.json) is set to msgbus
 
 ```javascript
    "config": {
        "source": "msgbus"
    }
 ```

* The template of respective pipeline is set to appsrc as source instead of uridecodebin

```javascript
    {
        "type": "GStreamer",
        "template": ["appsrc name=source",
                     " ! rawvideoparse",
                     " ! appsink name=destination"
                    ]
    }
```
