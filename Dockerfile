FROM alpine:3.13

RUN apk add --no-cache redis ca-certificates socat
RUN addgroup -S faktory && \
  adduser -S faktory -G faktory
RUN mkdir -p /etc/faktory /var/lib/faktory/db && \
  chown -R faktory:faktory /etc/faktory /var/lib/faktory
COPY faktory /bin/faktory

USER faktory
RUN mkdir -p /home/faktory/.faktory/db

EXPOSE 7419 7420
CMD ["/bin/faktory", "-w", "0.0.0.0:7420", "-b", "0.0.0.0:7419"]
