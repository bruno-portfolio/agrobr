ARG PYTHON_VERSION=3.11

# ---- Build ----
FROM python:${PYTHON_VERSION}-slim AS builder
ARG EXTRAS="browser,pdf"
WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY agrobr/ agrobr/
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip wheel --no-cache-dir --wheel-dir /wheels ".[${EXTRAS}]"

# ---- Runtime ----
FROM python:${PYTHON_VERSION}-slim
LABEL org.opencontainers.image.source="https://github.com/bruno-portfolio/agrobr"
LABEL org.opencontainers.image.description="Dados agrícolas brasileiros em uma linha de código"
LABEL org.opencontainers.image.licenses="MIT"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN groupadd --gid 1000 agrobr && \
    useradd --uid 1000 --gid agrobr --create-home agrobr
COPY --from=builder /wheels /tmp/wheels
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir --no-compile /tmp/wheels/*.whl && \
    rm -rf /tmp/wheels
RUN playwright install --with-deps chromium && \
    chown -R agrobr:agrobr /ms-playwright
USER agrobr
WORKDIR /home/agrobr
CMD ["python"]
