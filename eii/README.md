# Contents
- [Contents](#contents)
	- [Edge Video Analytics Microservice for OEI](#edge-video-analytics-microservice-for-oei)
	- [Prerequisites](#prerequisites)
	- [Run the containers](#run-the-containers)
	- [Configuration](#configuration)
		- [Config section](#config-section)
		- [Interfaces section](#interfaces-section)

## Edge Video Analytics Microservice for Open Edge Insights for Industrial

The Edge Video Analytics Microservice (EVAM) combines video ingestion and analytics
capabilities provided by Open Edge Insights for Industrial (Open EII) visual ingestion and analytics modules.
This directory provides the Intel® Deep Learning Streamer (Intel® DL Streamer) pipelines
to perform object detection on an input URI source and send the ingested frames and
inference results using the MsgBus Publisher. It also provides a Docker compose
and config file to use EVAM with the Open Edge Insights software stack.

>**Note:** In this document, you will find labels of ‘Edge Insights for Industrial (EII)’ for file names, paths, code snippets, and so on. Consider the references of EII as Open EII. This is due to the product name change of EII as Open EII.

## Prerequisites

As a prerequisite for EVAM, complete the following steps:

1. Run the following commands to get the Open EII source code:

   ```sh
    repo init -u "https://github.com/open-edge-insights/eii-manifests.git"
    repo sync
   ```

   >**Note:** For more details, refer [here](https://github.com/open-edge-insights/eii-manifests).

2. Complete the prerequisite for provisioning the Open EII stack by referring to the
[README.md](https://github.com/open-edge-insights/eii-core/blob/master/README.md#provision).
3. Download the required model files to be used for the pipeline mentioned in the [config](./config.json) file by completing step 2 to step 4 as mentioned in the [README](../README.md#running-the-image).
   >**Note:** The model files are large and hence they are not part of the repo.
4. Run the following commands to set the environment, build the `ia_configmgr_agent` container and copy models to the required directory:

   a. Go to the `build` directory:
      ```sh
      cd [WORK_DIR]/IEdgeInsights/build
      ```
   
   b. Configure visualizer app's subscriber interfaces as below.
      Example:
      Add below `interfaces` key value in WebVisualizer/config.json (the same holds true for visualizer apps like Grafana and Visualizer if one intends
      to see visualization of analyzed results in these apps, the below `interfaces` key value needs to be added to their respective config.json files in their
      directories):
   
      ```json
      "interfaces": {
	 "Subscribers": [
	    {
		"Name": "default",
		"Type": "zmq_tcp",
		"EndPoint": "ia_edge_video_analytics_microservice:65013",
		"PublisherAppName": "EdgeVideoAnalyticsMicroservice",
		"Topics": [
		    "edge_video_analytics_results"
		]
	    }
	 ]
       }
      ```
  
   c. Execute the builder.py script
   ```sh
   python3 builder.py -f usecases/evas.yml
   ```
   
   d. Create some necessary items for the service
   ```sh
   sudo mkdir -p /opt/intel/eii/models/
   ```
   
   e. Copy the downloaded model files to /opt/intel/eii
   ```sh
   sudo cp -r [downloaded_model_directory]/models /opt/intel/eii/
   ```

## Run the Containers

To pull the prebuilt Open EII container images and EVAM from Docker Hub and run the containers in the detached mode, run the following command:

```sh
# Launch the EII stack 
./eii_start.sh
```

> **Note:**
>
> The prebuilt container image for the [Edge Video Analytics Microservice](https://hub.docker.com/r/intel/edge_video_analytics_microservice)
> gets downloaded when you run the `docker-compose up -d` command, if the image is not already present on the host system.

## Configuration

See the [edge-video-analytics-microservice/eii/config.json](config.json) file for the configuration of EVAM. The default configuration will start the
object_detection demo for Open EII.

The config file is divided into two sections as follows:

### Config section

The following table describes the attributes that are supported in the `config` section.

|      Parameter      |                                                     Description                                                |
| :-----------------: | -------------------------------------------------------------------------------------------------------------- |
| `cert_type`         | Type of OEI certs to be created. This should be `"zmq"` or `"pem"`.                                            |
| `source`            | Source of the frames. This should be `"gstreamer"` or `"msgbus"`.                                              |
| `source_parameters` | The parameters for the source element. The provided object is the typical parameters.                          |
| `pipeline`          | The name of the DL Streamer pipeline to use. This should correspond to a directory in the pipelines directory).|
| `pipeline_version`  | The version of the pipeline to use. This typically is a subdirectory of a pipeline in the pipelines directory. |
| `publish_frame`     | The Boolean flag for whether to publish the metadata and the analyzed frame, or just the metadata.             |
| `model_parameters`  | This provides the parameters for the model used for inference.                 |

### Interfaces Section

Currently in the Open EII mode, EVAM supports launching a single pipeline and publishing on a single topic. This implies that in the configuration file ("config.json"), the single JSON object in the `Publisher` list is where the configuration resides for the published data. For more details on the structure, refer to the [Open EII documentation](https://github.com/open-edge-insights/eii-core/blob/master/README.md#add-oei-services).

EVAM also supports subscribing and publishing messages or frames using the Message Bus. The endpoint details for the Open EII service you need to subscribe from are to be provided in the **Subscribers** section in the [config](config.json) file and the endpoints where you need to publish to are to be provided in **Publishers** section in the [config](config.json) file.

To enable injection of frames into the GStreamer pipeline obtained from Message Bus, ensure to make the following changes:

- The source parameter in the [config](config.json) file is set to msgbus. Refer to the following code snippet:

  ```javascript
     "config": {
         "source": "msgbus"
     }
  ```

- The template of respective pipeline is set to appsrc as source instead of uridecodebin. Refer to the following code snippet:

  ```javascript
      {
          "type": "GStreamer",
          "template": ["appsrc name=source",
                       " ! rawvideoparse",
                       " ! appsink name=destination"
                      ]
      }
  ```
