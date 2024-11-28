# First Python App with Docker

## Try to run the app
```bash
pip install -r requirements.txt
python main.py
# ctrl + c to stop the app
```


## Containerize the app
```bash
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
