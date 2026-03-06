FROM dunglas/frankenphp

RUN install-php-extensions mysqli pdo_mysql

WORKDIR /app
COPY . /app

CMD sh -c 'SERVER_NAME=":${PORT:-8080}" frankenphp run --config /etc/caddy/Caddyfile --adapter caddyfile'
