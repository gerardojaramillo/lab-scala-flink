ARG KAFKA_VERSION=4.0.1

FROM apache/kafka:${KAFKA_VERSION}
USER root
RUN apk add --no-cache bash curl jq
USER 1000