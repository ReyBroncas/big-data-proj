# big-date-project

## Setup

`docker-compose up kafka cassandra mongo -d`

- create a topic in kafka:

    `./run-cluster.sh`

- run the rest of the services:
    
    `docker-compose up producer consumer aggregator`

- delete/stop:
    `docker-compose down && docker-compose rm`

--- 

#### Example

- Diagram of the project architecture:

    ![](/res/diagram.png)

- Examples
    
    ![](/res/ex1.png)
    ![](/res/ex2.png)

