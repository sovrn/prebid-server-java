# Builder image
FROM docker.rp-core.com/hub/maven:3.6.1-amazoncorretto-11 as builder

WORKDIR /app/prebid-server

RUN yum install -y rpm-build && yum clean all

COPY .git .git
COPY extra extra
COPY sample sample
COPY src src
COPY checkstyle.xml checkstyle.xml
COPY pom.xml pom.xml

ARG BUILD_NUMBER
ARG GIT_COMMIT

RUN mvn -f extra/pom.xml clean package -U \
        -Dci.build.number=${BUILD_NUMBER} \
        -Dci.repository.revision.number=${GIT_COMMIT};

# Final image
FROM docker.rp-core.com/rp_centos_7.3:1

RUN yum install -y java-11-amazon-corretto-devel-11.0.3.7-1 && yum clean all

RUN mkdir -p /app/prebid-server/conf /app/prebid-server/log /app/prebid-server/data/vendorlist-cache

COPY src/rpm/tcf-v1-fallback-vendorlist.json /app/prebid-server/conf/tcf-v1-fallback-vendorlist.json
COPY src/rpm/logback-spring.xml /app/prebid-server/conf/logback-spring.xml
COPY magnite/run.sh /app/prebid-server/run.sh

RUN chmod +x /app/prebid-server/run.sh

COPY --from=builder /app/prebid-server/extra/bundle/target/prebid-server-bundle.jar /app/prebid-server/prebid-server.jar

EXPOSE 8080

CMD [ "/app/prebid-server/run.sh" ]
