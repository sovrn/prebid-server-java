FROM docker.rp-core.com/rp_centos_7.3:1

# Install the rpm dependency for pulling secrets from the vault
RUN yum install -y rp-envconsul jdk-1.8.0_202-fcs jq
RUN yum clean all

RUN mkdir -p /app/prebid-server/conf /app/prebid-server/log /app/prebid-server/data/vendorlist-cache

COPY src/rpm/tcf-v1-fallback-vendorlist.json /app/prebid-server/conf/tcf-v1-fallback-vendorlist.json
COPY src/rpm/logback-spring.xml /app/prebid-server/conf/logback-spring.xml
COPY src/main/docker/run.sh /app/prebid-server/run.sh
RUN chmod +x /app/prebid-server/run.sh

COPY extra/bundle/target/prebid-server-bundle.jar /app/prebid-server/prebid-server.jar

EXPOSE 8080

CMD [ "/app/prebid-server/run.sh" ]
