#*********************************************************************
#   Copyright 2021 Regents of the University of California
#   All rights reserved
#*********************************************************************

ARG ECR_REGISTRY=ecr_registry_not_set

FROM ${ECR_REGISTRY}/merritt-tomcat:dev

COPY replication-war/target/mrt-replicationwar-*.war /usr/local/tomcat/webapps/replic.war

RUN mkdir -p /tdr/tmpdir/logs 
