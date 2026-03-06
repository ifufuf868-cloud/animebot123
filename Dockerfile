FROM dunglas/frankenphp:latest

# Install mysqli and pdo_mysql extensions
RUN install-php-extensions mysqli pdo_mysql

# Set the document root to /app
ENV SERVER_NAME=":80"

# Copy the application code
COPY . /app

# No custom CMD, use the default one from frankenphp image
