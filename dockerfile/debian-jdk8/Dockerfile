FROM debian:stretch

ADD jdk-8u211-linux-x64.tar.gz /opt/

ENV LANG C.UTF-8
ENV JAVA_HOME=/opt/jdk1.8.0_211
ENV CLASSPATH $JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
ENV PATH $PATH:$JAVA_HOME/bin


