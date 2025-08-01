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
