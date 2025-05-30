# How to upload Docker images to GCP

**NOTE:** For all the commands below, `PROJECT_NAME` is the name of the project in which you are working with hyphens instead of underscores. After you build the image you must **disable** `containerd` from the configurations page in Docker Desktop for the images to be built in `application/vnd.docker.distribution.manifest.v2+json` format.

## Building and testing the image
First of all, its necessary to build the image using:

```bash
docker build -t PROJECT_NAME .
```

Once the image is built, you can test it on local using
```bash
docker run -it --rm -u 0 --name PROJECT_NAME PROJECT_NAME bash
```
You can exit this run using `Ctrl+D`

## Uploading the image to GCP

Now that you are sure that the image works, you can upload it to GCP. To do this, you'll need to configure the connection, **this is only needed to be done once**:
```bash
gcloud auth configure-docker us-east1-docker.pkg.dev -q
```

Then to authenticate in GCP:

```bash
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://us-east1-docker.pkg.dev
```

Finally, to upload the image:

```bash
docker tag PROJECT_NAME us-east1-docker.pkg.dev/cl-bigdata-analytics/dataproc-worker-images/PROJECT_NAME:latest
docker push us-east1-docker.pkg.dev/cl-bigdata-analytics/dataproc-worker-images/PROJECT_NAME:latest
```