# Description

This folder provides a running configuration of elasticsearch-jdbc using elasticsearch and postgresql [docker](https://www.docker.com/) images.

# How to run the demonstration

First, be sure to be in the docker-example directory and to have docker installed. Please see [the Docker installation documentation](https://docs.docker.com/installation/) for details on how to install Docker.

Then, you need to run elasticsearch and postgresql containers:

```console
$ docker-compose up
```

When, elasticsearch is up and running and data have been populated in postgresql, then you can run elasticsearch-jdbc:

```console
$ docker-compose -f run.yml up
```

# Supported Docker versions

This example has been tested on Docker version 1.7.1 and docker-compose 1.3.3. 
