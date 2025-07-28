# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV POETRY_NO_INTERACTION=1
ENV POETRY_VENV_IN_PROJECT=1
ENV POETRY_CACHE_DIR=/opt/poetry-cache
ENV POETRY_HOME="/opt/poetry"
ENV POETRY_VERSION=1.8.5

# Add Poetry to PATH
ENV PATH="$POETRY_HOME/bin:$PATH"

# Install system dependencies needed for the project
RUN apt-get update && apt-get install -y \
    # Build tools and compilers
    build-essential \
    gcc \
    g++ \
    cmake \
    pkg-config \
    # ICU library for polyglot/pyicu
    libicu-dev \
    # Protocol Buffers
    protobuf-compiler \
    libprotobuf-dev \
    # SQLite
    sqlite3 \
    libsqlite3-dev \
    # OpenCV dependencies
    libopencv-dev \
    python3-opencv \
    # Additional libraries that may be needed
    libjpeg-dev \
    libpng-dev \
    libtiff-dev \
    libwebp-dev \
    libhdf5-dev \
    libssl-dev \
    libffi-dev \
    # Git for dependencies from git repos
    git \
    # Curl for downloading
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 - --version $POETRY_VERSION

# Set work directory
WORKDIR /app

# Copy Poetry configuration files
COPY pyproject.toml poetry.lock* ./

# Configure Poetry and install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi \
    && rm -rf $POETRY_CACHE_DIR

# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p data/input data/output

# Expose port if needed (adjust as necessary)
EXPOSE 8000

# Set the default command
CMD ["python", "pipeline.py", "--help"]