FROM registry.cn-shanghai.aliyuncs.com/aliware2019/aliware:debian-jdk8-devel

COPY service-consumer-1.0.0.jar /usr/local/service/

COPY start.sh /usr/local/service

WORKDIR /usr/local/service

ENTRYPOINT ["sh","start.sh"]

