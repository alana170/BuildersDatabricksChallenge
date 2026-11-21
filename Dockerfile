# Build the specialized processing engine for the scorer's Cascade Lake-class x86 CPUs.
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src/pipeline.cpp /src/pipeline.cpp
RUN g++ -std=c++20 -O3 -DNDEBUG -pthread -march=cascadelake -mtune=cascadelake \
    -pipe /src/pipeline.cpp -o /pipeline

FROM python:3.12-slim-bookworm
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app \
    && mkdir -p /app /scratch /output \
    && chown -R 1000:1000 /scratch /output
COPY --from=build /pipeline /app/pipeline
COPY scripts/convert.py /app/convert.py
COPY run.sh /app/run.sh
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt && chmod +x /app/run.sh
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
USER 1000:1000
CMD ["/app/run.sh"]
