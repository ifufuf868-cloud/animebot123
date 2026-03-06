FROM dunglas/frankenphp

# Install mysqli and pdo_mysql extensions
RUN install-php-extensions mysqli pdo_mysql

COPY . /app
