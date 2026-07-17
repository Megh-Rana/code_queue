FROM python:3.12-slim

WORKDIR /app

# Install language runtimes for real code execution
# golang is installed via official tarball (apt version is often outdated)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    nodejs \
    npm \
    ruby \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Go from official tarball
ENV GO_VERSION=1.22.4
RUN curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" \
    | tar -C /usr/local -xz \
    && ln -s /usr/local/go/bin/go /usr/local/bin/go

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker.py .

CMD ["python", "worker.py"]
