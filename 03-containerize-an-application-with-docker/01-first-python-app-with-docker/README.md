# First Python App with Docker

## Try to run the app

```bash
cd /workspaces/data-ai-bootcamp/03-containerize-an-application-with-docker/01-first-python-app-with-docker

pip install -r requirements.txt

python main.py
# ctrl + c to stop the app
```

## Containerize the app

```bash
# check version docker
docker --version

# Build the image
docker build -t myapp .

# List all images
docker images

# Run the container
docker run -d --name generate-customer-data myapp

# List all containers
docker ps -a

# Show logs
docker logs --tail 10 generate-customer-data

# Stop the container
docker stop generate-customer-data

# Remove the container
docker rm generate-customer-data

# Remove the image
docker rmi myapp
```

## Congratulation!!
