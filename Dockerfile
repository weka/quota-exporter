FROM ubuntu:20.04


ARG BASEDIR="/weka"
ARG ID="472"
ARG USER="weka"

RUN adduser --home $BASEDIR --uid $ID --disabled-password --gecos "Weka User" $USER

WORKDIR $BASEDIR

COPY ./tarball/quota-export/quota-export $BASEDIR

EXPOSE 8101

USER $USER
ENTRYPOINT ["/weka/quota-export"]
