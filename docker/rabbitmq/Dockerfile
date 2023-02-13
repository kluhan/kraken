FROM rabbitmq:3.9.5-management-alpine

# Define environment variables
ENV RABBITMQ_USER default_user
ENV RABBITMQ_PASSWORD default_password
ENV RABBITMQ_PID_FILE /var/lib/rabbitmq/mnesia/rabbitmq

# Add custome init-script
ADD init.sh /init.sh
RUN chmod +x /init.sh

# Expose managemant and default port
EXPOSE 15672 5672

WORKDIR /
COPY 10-default-guest-user.conf ./etc/rabbitmq/conf.d/10-default-guest-user.conf

# Define default command
CMD ["/init.sh"]