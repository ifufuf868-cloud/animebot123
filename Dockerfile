FROM dunglas/frankenphp:latest-php8.3

# Install mysqli and pdo_mysql extensions
RUN install-php-extensions mysqli pdo_mysql

# Set the document root to /app
WORKDIR /app
COPY . /app

# Set port 80 as default if PORT is not provided (standard frankenphp behavior)
ENV SERVER_NAME=":80"

# Use the custom Caddyfile
CMD ["frankenphp", "run", "--config", "/app/Caddyfile", "--adapter", "caddyfile"]
