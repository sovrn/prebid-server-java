FROM docker.rp-core.com/rp_centos_7.3:1

# Install the rpm dependency for pulling secrets from the vault
RUN yum install -y rp-envconsul jdk-1.8.0_202-fcs jq
RUN yum clean all

RUN mkdir -p /app/prebid-server/conf /app/prebid-server/log /app/prebid-server/data/vendorlist-cache

COPY src/main/docker/run.sh /app/prebid-server/run.sh
RUN chmod +x /app/prebid-server/run.sh

COPY target/prebid-server.jar /app/prebid-server/prebid-server.jar

EXPOSE 8080

CMD [ "/app/prebid-server/run.sh" ]
