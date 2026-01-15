docker build --build-arg MODULE_NAME=leader -t graph-leader:v1 .
docker build --build-arg MODULE_NAME=worker -t graph-worker:v1 .