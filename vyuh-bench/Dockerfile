FROM rust:1.92-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql \
        postgresql-client \
        fonts-dejavu-core \
        python3 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/lib/postgresql/15/bin:/usr/local/cargo/bin:${PATH}"
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /work
COPY . .

RUN useradd -m bench \
    && chown -R bench:bench /work

USER bench

RUN python3 -m venv vyuh-bench/.venv \
    && vyuh-bench/.venv/bin/python -m pip install --no-cache-dir -r vyuh-bench/requirements.txt

ENTRYPOINT ["vyuh-bench/.venv/bin/python", "vyuh-bench/scripts/run_bench.py"]
