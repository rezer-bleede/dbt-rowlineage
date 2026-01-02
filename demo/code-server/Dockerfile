FROM codercom/code-server:4.91.1

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER coder

WORKDIR /home/coder/project
ENV PASSWORD=demo

EXPOSE 8443

CMD ["code-server", "--bind-addr", "0.0.0.0:8443", "/home/coder/project"]
