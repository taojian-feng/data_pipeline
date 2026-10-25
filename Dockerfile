# Local / batch image: `docker run --rm -v gridlake:/data gridlake run --days 14`
FROM python:3.12-slim AS build
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY contracts ./contracts
COPY seeds ./seeds
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.12-slim
RUN useradd --create-home --uid 10001 gridlake && mkdir /data && chown gridlake:gridlake /data
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
USER gridlake
ENV GRIDLAKE_HOME=/data
WORKDIR /home/gridlake
VOLUME /data
ENTRYPOINT ["gridlake"]
CMD ["run", "--days", "14"]
