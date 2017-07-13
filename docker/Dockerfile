# Base image Ubuntu 16.04
FROM ubuntu:16.04

# Make a directory /app
WORKDIR /app

ADD . /app

# Library dependencies
RUN apt-get update && apt-get install -y \
    git \
    cmake \
    vim \
    build-essential \
    wget \
    g++ \
    libboost-all-dev \
    liblz4-dev \
    libtbb-dev \
    libsparsehash-dev \
    python3 \
    python3-dev \
    python-numpy \
    libcurl4-openssl-dev \
    pkg-config \
    libgranite-dev \
    libindicator3-dev \
    libkeybinder-3.0-dev \
    libvte-dev

# Get Trident code and build it
RUN git clone https://github.com/jrbn/trident /app/trident
RUN cd /app/trident; \
    mkdir /app/trident/build; \
    cd /app/trident/build; \
    cmake ..; \
    make

# Get vlog
RUN git clone https://github.com/jrbn/vlog /app/vlog
RUN cd /app/vlog; \
    make

CMD ["/bin/bash"]
