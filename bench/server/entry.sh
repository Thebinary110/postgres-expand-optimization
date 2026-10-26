#!/bin/bash
# Creates the SSH account (password from env) once, starts sshd, then hands over to the stock postgres entrypoint.
set -e
if ! id harness >/dev/null 2>&1; then
  useradd -m -s /bin/bash harness
  echo "harness:${SSH_PASSWORD}" | chpasswd
  echo 'PasswordAuthentication yes' > /etc/ssh/sshd_config.d/harness.conf
fi
/usr/sbin/sshd
exec docker-entrypoint.sh "$@"
