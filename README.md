# Kraken
Kraken is an open source tool for building vertical web crawlers. It was developed at [CISPA](https://cispa.de) to fill the capability gap between tools like [Scrapy](https://scrapy.org/) and more powerful and complex general purpose crawler like [Heretic3](https://github.com/internetarchive/heritrix3). 
It attempts to provide a holistic toolchain where the end user only needs to provide functions to extract data from the target data source, while Kraken takes care of resource allocation, efficient data storage, and task distribution across multiple workers.
Currently, Kraken is in a very early stage of development and only supports crawling the [Google Play Store](https://play.google.com/store/apps). However, the architecture is designed to be easily extensible to other data sources.

## Features
* **Efficient data storage**: Kraken uses MongoDB to store the crawled data as efficiently as possible. It automaticity applies a reverse delta encoding to the crawled data points if a data point if visited multiple times.
* **Resource allocation**: Kraken provides multiple strategies to allocate resources to the different pages of a data source.
* **Distributable**: Kraken can be distributed across multiple servers out of the box.
* **Docker-Compose support**: Kraken provides a docker-compose file to easily deploy the crawler and all necessary services on a single machine.
* **Easy to extend**: Kraken is designed to be easily extensible to other data sources.
* **Google Play Store support**: Kraken provides functions to crawl the following information from the Google Play Store:
    * **General App Information**: All information that is available on the app's main page, such as the app's name, description, developer, rating, etc. 
    * **App Permissions**: All permissions that are requested by the app. *Note: As of 2021, permissions can no longer be accessed through the Google Play Store website, but only through the Google Play Store API. As a result, this feature may be considered deprecated and removed at any time.*
    * **App Data Safety Information**: All information that is available on the app's data safety page, such as the app's privacy policy, data collection, etc.
    * **App Reviews**: All reviews that are available on the app's review page. *Note: Until now, it is possible to extract all reviews ever written for an app.*


## Use Cases
The Kraken is intended for use in scenarios where most of the following apply:

- You want to crawl the Google Play Store or another vertical data source.
- You require a larger amount of data that cannot be easily obtained through a basic for-loop crawler.
- You do not require the throughput of a full-sized general-purpose crawler.
- You want to gather longitudinal data by tracking changes over a extended period of time. *Not strictly necessary, but one of the main selling points of the Kraken.*

If not most of the points above apply, you may consider using a different crawler, like [Scrapy](https://scrapy.org/) or [Heritrix3](https://github.com/internetarchive/heritrix3).

## Core concepts

### Targets
A target is a set of parameters that locate a record in the data source. In most cases a target can be build by extracting the parameters from the URL which locates the record. For example, the Target for the german [YouTube-App](https://play.google.com/store/apps/details?id=com.google.android.youtube) in the Google Play Store is {`"id": "com.google.android.youtube", "lang": "de"}`. In some cases records are only accessible in a fragmented manner, like the Google Play Store which provides on page for each app with general information and a separate page for security information. In this case a single Target contains all parameters to access all fragments of a record.

### request-Functions
A request function is a function that takes a Target as input and returns a parsed fragment of the record. In the example of the Google Play Store, multiple such functions exist. One function to extract the general information, one function to extract the security information, and so on. 

### Stages
While Targets define the records to be crawled, Stages define how a Crawl should be performed. A Stage is a set of request functions that should be executed in a specific order as well as a list of pipelines and callbacks to process the loaded data.

## Installation
This section describes how to install the Kraken on a single machine.

### Prerequisites
To run the Kraken, you need to have a working installation of [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/). Alternatively, you can use [Rancher Desktop](https://rancherdesktop.io/), a Docker desktop alternative for Windows, Mac, and Linux. If neither is available, you can try [Podman](https://podman.io/) as an untested alternative, which is a drop-in replacement for Docker and seems to work better in some cases on the M1 architecture.

### Step 1: Summon the Kraken!
To install the **Kraken**, you basically need two things:
the **Docker Image** and and some **Configuration File**.

The simplest way to get the both is to clone the complete repository via:

    git clone https://projects.cispa.saarland/c01kllu/google-play-kraken.git

In this case the **Docker Image** is build automatically and all **Configuration Files** beside the `docker-compose.yml` are located in the `env` folder.
The following files are important:

- **docker-compose.yml**: The docker-compose file that defines the services and their configuration. Make sure to adjust the concurrency settings of the two workers **scylla** and **charybdis** to your hardware and needs.
- **env/elastic.env**: The environment file for the Elasticsearch service. This should work out of the box, but you can adjust the configuration to your needs.
- **env/flower.env**: The environment file for the Flower service. Here you can adjust the port for the Flower UI.
- **env/leek.env**: The environment file for the Leek service. For more details, see the [Leek documentation](https://github.com/kodless/leek).
- **env/mongodb.env**: The environment file for the MongoDB service. Adjust the user and password.
- **env/rabbitmq.env**: The environment file for the RabbitMQ service. Adjust the user and password.
- **env/worker.env**: The environment file for the Kraken Worker service. Make sure to adjust the user and password according to the ones you set in the RabbitMQ and MongoDB environment file.

The default settings are not secure and should be changed before using the **Kraken** in production but should work out of the box for testing purposes.

### Step 2: Pull the Services
To pull the services, you can use the command:

    docker-compose pull

This command will pull all the necessary images, like MongoDB, Redis and RabbitMQ from the DockerHub registry. 

## Step 3: Start the Services
Once all images are available, you can start the services. To do so, use the command:

    docker-compose up -d

This command starts all the necessary containers in the background, so they don't shut down when you close the terminal. To start only a specific container, use `docker compose up <Service Name>`.


## Step 4: Check the Services
After starting the services, you should check if everything is running as intended. This can be done in several ways.

### Step 4.1: Check the Services with `docker compose ps`
The easiest way to check if all services are running is to use the command

    docker-compose ps

This command will list all running services and their status. 
If a service is not running, you can check the logs of the service to see what went wrong. To do so, use the command:

    docker compose logs <Service Name>

### Step 4.2: Check the Services with **Portainer**
If you prefer a GUI to check the status of the services, you can use [Portainer](https://www.portainer.io/). Portainer is a web-based tool that allows you to manage your Docker containers, images, networks, and volumes. To use Portainer, you need to start the Portainer container first. To do so, use the command:

    docker run -d -p 9000:9000 -v /var/run/docker.sock:/var/run/docker.sock portainer/portainer

This command will start the Portainer container and expose it on port 9000. To access the Portainer UI, open the following URL: `http://localhost:9000`.

Once you have opened the Portainer UI, you can create an account and login. After logging in, you should see the Portainer dashboard which allows you to manage your Docker containers, images, networks, and volumes as well as the logs of the services.

### Step 4.3: Check your Workers with **Flower**
If your services are running as intended, you can check if the workers are running as well and are connected to the broker. To do so, you can use the built-in monitoring tool **Flower**. To access the Flower UI, open the following URL: `http://localhost:8888`.
After opening the Flower UI, you should see the Flower dashboard with two registered workers "scylla" and "charybdis". If you don't see the workers, you can check the logs of the two workers to see what went wrong. 

## Step 5: Setup Leek *(Optional)*
The Kraken comes with two monitoring tools: **Flower**, which you already used in the previous step, and **Leek**. **Leek** provides a more detailed view of the workers and their tasks than **Flower** but requires some additional configuration. To do so, open `http://localhost:8000` in your browser and create a new application "kraken" with the description "Kraken". After a few seconds, both workers "scylla" and "charybdis" should be visible in the dashboard.

# Crawling the Google Play Store: A Quickstart guide
This section describes how to crawl the Google Play Store with the Kraken. The Kraken comes with a set of predefined targets and stages that can be used to crawl the Google Play Store.

## Prerequisites
Before you can start crawling the Google Play Store, it is recommended install poetry, a dependency manager for Python. Instructions on how to install poetry can be found [here](https://python-poetry.org/docs/#installation). Once poetry is installed, you can install the dependencies for the Kraken by running the following command in the root directory of the Kraken:

    poetry install

If installing poetry is not an option, you can use the Scylla or Charybdis container, as they already contain all the necessary dependencies. To do so, you can use the following command:

    docker exec -it gpk_scylla bash

If you use the shell inside the container, you can omit the `poetry run` command in the following steps.

## Step 1: Add Targets to the Database
The first step is to add some targets to the MongoDB database. This can be done by using the `kraken_cli.py` script. To do so, use the command: 
    
    poetry run python kraken_cli.py setup-targets --tag="initial" ./resources/targets/gps_list_small.json en de

This command will add the targets from the file `./resources/targets/gps_list_small.json` for the languages English and German to the MongoDB database. The targets are tagged with the tag "initial". Tags are used to group targets and allows to crawl only a subset of the known targets. If you want to add targets in a different language, you can use the ISO 639-1 code of the language. For a list of all available languages, see [here](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes). For more information on how to use the `kraken_cli.py` script, use the command `poetry run python kraken_cli.py setup-targets --help`.

The following three lists of targets are included in the Kraken:
* `./resources/targets/gps_list_small.json`: A list of 31 Apps that can be used for testing purposes.
* `./resources/targets/gps_list_medium.json`: A list of 104311 Apps.
* `./resources/targets/gps_list_large.json`: A list of 2516756 Apps, representing 99% of all Apps in the Google Play Store as of 2022-09-01.

## Step 1: Define the Stages of the Crawl
While Targets define the records to be crawled, Stages define how a Crawl should be performed. The Kraken comes with a set of predefined stages that can be used to crawl the Google Play Store. To use the predefined stages, you can use the command:

    poetry run python kraken_cli.py setup-series \
        --description="A Series created by by the Quickstart guide" \
        --stage="resources/stages/stage_gps_detail_full.json" \
        --stage="resources/stages/stage_gps_review_static.json" \
        --filter="resources/filter/target_filter_example_0.json" \
        QuickstartGuideSeries

This command will add a new series to the MongoDB database with the description "A Series created by by the Quickstart guide" and the name "QuickstartGuideSeries". The series contains two stages: `stage_gps_detail_full.json` and `stage_gps_reviews_static.json`. The first stage will crawl the details of the Apps and the second stage will crawl the first 1000 reviews of the Apps. The stages are executed in the order they are defined in the command. The `--filter` parameter defines a subset of the known targets for the crawl. In this case, the filter will only select targets that have the tag "initial". For more information on how to use the `kraken_cli.py` script, use the command `poetry run python kraken_cli.py setup-series --help`.

## Step 2: Start the Daemon
After the targets and the series, containing the stages and the filter, have been added to the database, the daemon can be started. To do so, use the command:

    poetry run python kraken_cli.py daemon --is_name QuickstartGuideSeries

This command will start a daemon that will execute the specified series by submit tasks to the Workers. The daemon will run until all targets have been crawled or until the daemon is stopped. For more information on how to use the `kraken_cli.py` script, use the command `poetry run python kraken_cli.py daemon --help`.

# Tips, Tricks and known Pitfalls
- Since the host network driver is not available on macOS, the docker-compose.yml file is configured to use the bridge driver. Therefore, to access a service within a container from **within** a container, you can no longer use `localhost` even if the service is technically running on the same machine. Instead, use the name of the service as the host name. For example, to access the Redis service from the google-play-kraken service, use `redis` as the hostname. For external access, i.e. access from outside a docker container, localhost can still be used.
- Many parameters can be configured via environment variables. For a list of all available environment variables, see the docker-compose.yml file and the corresponding `<Service>.env`-files located in the `env` directory.

# Included Services and their Purpose
This section gives a brief overview of the services included in the docker-compose.yml file and their purpose.

| Service   | Default Port | Description                                 | User Interaction |
| --------- | ------------ | ------------------------------------------- | ---------------- |
| Scylla    | -            | Worker for non-blocking tasks               | CLI              |
| Charybdis | -            | Worker for blocking tasks                   | CLI              |
| Flower    | 8888         | The built-in monitoring tool for Celery     | GUI              |
| RabbitMQ  | 15672, 5672  | The message broker used by the Kraken       | GUI              |
| MongoDB   | 27017        | The database used by the Kraken             | No               |
| Redis     | 6379         | The result store used by the Kraken         | No               |
| Leek      | 8000         | A more advanced monitoring tool for Celery  | GUI              |
| Elastic   | 9200, 9300   | The database used by Leek to store the logs | No               |