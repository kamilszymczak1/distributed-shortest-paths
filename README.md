## Running the app locally

### Running the Java programs on their own

To run the app locally, you are going to need Maven and Docker Desktop instaled
on your machine.

To build Java apps just run
```
mvn clean install
```
This should create two `.jar` files: `leader/target/leader-1.0-SNAPSHOT.jar` and
`worker/target/worker-1.0-SNAPSHOT.jar`, which you can run with
```
java -jar worker/target/worker-1.0-SNAPSHOT.jar
```
and similarly for the leader. However ***running the apps this way is discouraged***
as it would be quite difficult to make the workers communicate with the leader. Instead,
you can create Docker containers that will make setting everything up quite seamless.

### Running everything at once with Docker

To create the Docker containers run
```
chmod +x docker_build.sh
./docker_build.sh
```
after that you can just run
```
docker compose up
```
which will run the leader and the workers. You can observe
their outputs in the terminal.

### Downloading the graph
In order for the leader to read the graph, you have to download it first.
You can download it [here](https://www.diag.uniroma1.it/challenge9/download.shtml).
You need to put the file with coordinates under `./data/graph.co.gz` and the one
with arcs under `./data/graph.gr.gz` for the Docker containers to work properly.

## Runnig the app on GCE
Not available yet...