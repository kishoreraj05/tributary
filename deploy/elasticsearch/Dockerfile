# A single-node Elasticsearch for developing and testing the Elasticsearch sink locally:
# security off, no TLS, 512 MB heap, listening on 9200. Never run this configuration
# anywhere reachable from the outside.
#
#   docker build -t tributary/elasticsearch deploy/elasticsearch
#   docker run -d --name tributary-es -p 9200:9200 tributary/elasticsearch
#
# `docker compose -f deploy/docker-compose.yml up -d elasticsearch` builds and runs the same image.
ARG ES_VERSION=8.13.4
FROM docker.elastic.co/elasticsearch/elasticsearch:${ES_VERSION}

ENV discovery.type=single-node \
    xpack.security.enabled=false \
    xpack.security.enrollment.enabled=false \
    cluster.name=tributary \
    ES_JAVA_OPTS="-Xms512m -Xmx512m"

EXPOSE 9200

HEALTHCHECK --interval=5s --timeout=3s --retries=30 \
  CMD curl -fs http://localhost:9200/_cluster/health || exit 1
