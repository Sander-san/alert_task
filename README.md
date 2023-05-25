This script analyzes the logs for errors and warns about a given number of fatal errors within a given time.

The code runs in parallel based on the multiprocessing library and can be run in a container.
To run:
  1) docker build -t <image_name>
  2) docker run --rm --name <container_name> <image_name>

Data files were not included.
