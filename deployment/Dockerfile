FROM phusion/baseimage:0.9.19
MAINTAINER Tom Coupland <tom.coupland@mastodonc.com>

CMD ["/sbin/my_init"]

RUN apt-get install software-properties-common
RUN add-apt-repository -y ppa:webupd8team/java \
&& apt-get update \
&& echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
&& apt-get install -y \
software-properties-common \
oracle-java8-installer \
oracle-java8-set-default

ENV JAR_LOCATION=/srv/kixi.datastore.jar
ENV CONFIG_PROFILE=${environment}

ADD target/kixi.datastore-standalone.jar /srv/kixi.datastore.jar
ADD scripts/run.sh /etc/service/kixi.datastore/run

EXPOSE 8080/tcp

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
