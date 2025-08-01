<<<<<<< HEAD
FROM python:3.9-slim-buster

WORKDIR /app

RUN sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
    sed -i 's|security.debian.org/debian-security|archive.debian.org/debian-security|g' /etc/apt/sources.list && \
    sed -i '/buster-updates/d' /etc/apt/sources.list

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgstreamer1.0-0 \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-libav \
    libgirepository1.0-dev \
    gcc \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# The command to run the service will be specified in docker-compose.yml
=======
# Use the full, non-slim version of the Debian Bullseye image for better compatibility
FROM python:3.9-bullseye

# Install GStreamer and its Python bindings (PyGObject) using apt
# This is more reliable than compiling with pip
RUN apt-get update && apt-get install -y --no-install-recommends \
    # GStreamer runtime libraries
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    # Add the GStreamer tools for better plugin discovery
    gstreamer1.0-tools \
    # The pre-compiled Python bindings for GStreamer and GObject
    python3-gi \
    python3-gst-1.0 \
    gir1.2-gst-plugins-base-1.0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Install the remaining Python packages using pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
>>>>>>> Vipul
