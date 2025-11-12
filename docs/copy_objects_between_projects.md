# Copying objects from one project to another

## Secret manager secrets
To copy images from one project to another the team has developed a `copy_secrets.py` script. The script must be called from the root directory of the repo, using the following line:

``` bash
python src\common\local\copy_secrets.py SOURCE_PROJECT TARGET_PROJECT SECRET_NAME[...]
```

Where you can put all the secret names you want to be copied one after another and the script will copy them one after another. For example, the call

``` bash
python src\common\local\copy_secrets.py cl-bigdata-analytics-preprod cl-bigdata-analytics-prod secret_1 secret_2
```

Will copy the secrets `secret_1` and `secret_2` from the project `cl-bigdata-analytics-preprod` to `cl-bigdata-analytics-prod`

## Artifact registry images
The fastest way to copy artifact registry images from one project to another is to use script `copy_images.py`. For this script to work you'll need to download the developed by Google open-source utility `gcrane`.

### Installing `gcrane`
You can found the builds of this tool on the [releases page of the go-containerregistry](https://github.com/google/go-containerregistry/releases) Git repository. Here you'll need to download the version that is compatible with your system (most probably the Windows_x86_64.tar.gz).

After unpacking the file you'll be greated with three different tools (at the time of writting this documentation, this can change though), from the three tools you'll only need the `gcrane.exe` file, which should be located on the root directory of the repository (this is the same level as the src directory).

### Using the `copy_images.py` script

The script must be called from the root directory of the repo, using the following line:

``` bash
python src\common\local\copy_images.py SOURCE_PROJECT TARGET_PROJECT IMAGE_NAME[...]
```

Where you can put all the image names you want to be copied one after another and the script will copy them one after another. For example, the call

``` bash
python src\common\local\copy_images.py cl-bigdata-analytics-preprod cl-bigdata-analytics-prod image_1 image_2
```

Will copy the images `image_1` and `image_2` from the project `cl-bigdata-analytics-preprod` to `cl-bigdata-analytics-prod`
