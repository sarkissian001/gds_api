# Streamer API

This is a Spring Boot application designed to interact with Kafka streams, providing an API to check for the existence of records in a Kafka topic. It's built using Spring Kafka, Kafka Streams, and includes Swagger for API documentation.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Technologies](#technologies)
- [Setup](#setup)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Testing](#testing)


## Introduction

The Streamer API offers a way to interact with Kafka streams. It allows users to query the state store of a Kafka Streams application to check if a particular record ID exists.

## Features

- Interaction with Kafka Streams.
- Query Kafka state stores.
- API documentation using Swagger.

## Technologies

- Spring Boot
- Kafka Streams
- Spring Kafka
- Swagger (Springfox)
- Docker

## Setup

### Prerequisites

- Java 8 or higher
- Kafka Cluster
- Docker and Docker Compose

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/streamer-api.git
    ```

2. Navigate to the project directory:
    ```bash
    cd streamer-api
    ```

3. Build the project:
    ```bash
    ./gradlew build
    ```

## Usage

### Running the Application

#### With Docker Compose

Ensure Docker is installed and running on your system. Then, use Docker Compose to build and start the services:

```bash
docker-compose up -d
```

### Without Docker

Alternatively, you can run the application using:

```bash
./gradlew bootRun
```

### Populating Kafka Topic

To populate the Kafka topic with data, you can run the `produce_data.py` script. Note that you need to install the required library to execute this script, or alternatively, you can create and populate the topic manually.

```bash
python produce_data.py
```

## Testing the API

You can test the API using the following `curl` command:

```bash
curl "http://localhost:8080/v1/id-lookup?record_id=100"
```

