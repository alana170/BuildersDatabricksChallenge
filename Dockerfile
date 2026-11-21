# Build one static native binary; run it in an empty image (no shell, no Python, no startup cost).
FROM debian:bookworm-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends g++ libc6-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY src/pipeline.cpp /src/pipeline.cpp
# x86-64-v3 = AVX2/BMI2 (every Cascade Lake has it); tuned for the scorer's Cascade Lake CPUs.
RUN g++ -std=c++20 -O3 -DNDEBUG -pthread -march=x86-64-v3 -mtune=cascadelake -static \
    -pipe /src/pipeline.cpp -o /pipeline

FROM scratch
COPY --from=build /pipeline /pipeline
USER 1000:1000
CMD ["/pipeline"]
