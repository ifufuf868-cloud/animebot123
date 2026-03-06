FROM dunglas/frankenphp

# Install mysqli and pdo_mysql extensions
RUN install-php-extensions mysqli pdo_mysql

# Set the document root to /app
ENV SERVER_NAME=":80"

WORKDIR /app
COPY . /app
