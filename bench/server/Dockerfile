# Linux side of the challenge: PostgreSQL 16 + OpenSSH server + psql/pg_dump/pg_restore + python3 (generator).
FROM postgres:16-bookworm
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        openssh-server python3 procps time && rm -rf /var/lib/apt/lists/* && mkdir -p /run/sshd
COPY entry.sh /usr/local/bin/entry.sh
RUN chmod +x /usr/local/bin/entry.sh
ENTRYPOINT ["/usr/local/bin/entry.sh"]
