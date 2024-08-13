#*********************************************************************
#   Copyright 2021 Regents of the University of California
#   All rights reserved
#*********************************************************************

ARG ECR_REGISTRY=ecr_registry_not_set

FROM ${ECR_REGISTRY}/merritt-tomcat:dev
ARG COMMITDATE=''

COPY replication-war/target/mrt-replicationwar-*.war /usr/local/tomcat/webapps/replic.war

RUN mkdir -p /build/static && \
    echo "mrt-replic: ${COMMITDATE}" > /build/static/build.content.txt && \
    jar uf /usr/local/tomcat/webapps/replic.war -C /build static/build.content.txt

RUN mkdir -p /tdr/tmpdir/logs 
