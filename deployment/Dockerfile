FROM zittix/docker-baseimage-java8
MAINTAINER Tom Coupland <tom.coupland@mastodonc.com>

CMD ["/sbin/my_init"]

RUN apt-get update \
&&  apt-get -y install software-properties-common

ENV JAR_LOCATION=/srv/kixi.datastore.jar
ENV CONFIG_PROFILE=${environment}

ADD target/kixi.datastore-standalone.jar /srv/kixi.datastore.jar
ADD scripts/run.sh /etc/service/kixi.datastore/run

EXPOSE 8080/tcp
EXPOSE 5002/tcp

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
