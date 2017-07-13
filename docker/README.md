# How to build and run

To build the docker image, make a directory say MyApp and put your Dockerfile there. The name must be Dockerfile. Then run the following command from inside the directory.
```
docker build -t <TAG> .
```

You can see the docker images using following command
```
docker images
```

To run the container from the image, execute following command

```
docker run -it <TAG>
```

You can tag the image with your docker hub user ID with following command
```
docker tag <LOCAL-TAG> <DOCKER-ID>/<REMOTE-TAG>
```


To push the image in your docker hub repository, use following command
```
docker push <DOCKER-ID>/<REMOTE-TAG>
```
