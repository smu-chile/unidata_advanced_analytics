# Debian 11 is recommended.
FROM debian:11-slim

# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# (Required) Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini libjemalloc2 wget

# (Optional) Install utilities required by XGBoost for Spark.
RUN apt install -y procps libgomp1

# Enable jemalloc2 as default memory allocator
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# (Optional) Add extra jars.
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
ENV SPARK_EXTRA_CLASSPATH='/opt/spark/jars/*'
RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"

# Uncomment below and replace EXTRA_JAR_NAME with the jar file name.
# COPY "EXTRA_JAR_NAME" "${SPARK_EXTRA_JARS_DIR}"

# (Optional) Install and configure Miniconda3.
ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
RUN wget -O miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-py312_25.3.1-1-Linux-x86_64.sh
RUN bash miniconda.sh -b -p /opt/miniconda3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --prepend channels conda-forge \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict
# Packages ipython and ipykernel are required if using custom conda and want to
# use this container for running notebooks.
#RUN ${CONDA_HOME}/bin/conda install ipython ipykernel

# (Optional) Install Conda packages.
#
# The following packages are installed in the default image, it is strongly
# recommended to include all of them.
#
# Use mamba to install packages quickly.
RUN ${CONDA_HOME}/bin/conda install -y micromamba -n base -c conda-forge \
    && ${CONDA_HOME}/bin/micromamba install -y -c conda-forge\
      conda \
      cython \
      fastavro \
      fastparquet \
      gcsfs \
      google-cloud-bigquery-storage \
      google-cloud-bigquery[pandas] \
      google-cloud-bigtable \
      google-cloud-container \
      google-cloud-datacatalog \
      google-cloud-dataproc \
      google-cloud-datastore \
      google-cloud-logging \
      google-cloud-monitoring \
      google-cloud-pubsub \
      google-cloud-redis \
      google-cloud-secret-manager \
      google-cloud-spanner \
      google-cloud-storage \
      koalas \
      matplotlib \
      nltk \
      numba \
      numpy \
      openblas \
      orc \
      "pandas=2.1.*" \
      pyarrow \
      pytables \
      python \
      regex \
      requests \
      rtree \
      scikit-learn \
      scipy \
      seaborn \
      sqlalchemy \
      sympy \
      pendulum \
      awswrangler \
      pandas-gbq \
      virtualenv \
    && ${CONDA_HOME}/bin/micromamba clean --all --yes

# (Optional) Install additional requirements
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# Set timezone
ENV TZ="America/Santiago"

# (Required) Create the 'spark' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark
