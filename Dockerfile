FROM amazoncorretto:11.0.7 as build

WORKDIR /build/ 

RUN yum install -y wget which ant

COPY . /build/

RUN ./setup --build


FROM amazoncorretto:11.0.7 as final

WORKDIR /agent/

COPY --from=build /build/dependencies/ /agent/dependencies/
COPY --from=build /build/ant_build/lib/AWSKinesisStreamingDataAgent-1.1.jar /agent/
COPY --from=build /build/configuration/release/aws-kinesis-agent.json /etc/aws-kinesis/agent.json 
COPY --from=build /build/support/log4j.xml /agent/log4j.xml

RUN mkdir -p /var/log/aws-kinesis-agent/ \
    && touch /var/log/aws-kinesis-agent/aws-kinesis-agent.log \ 
    && ln -sf /dev/stdout /var/log/aws-kinesis-agent/aws-kinesis-agent.log
    
CMD ["java", "-Dlog4j.configurationFile=/agent/log4j.xml","-cp","AWSKinesisStreamingDataAgent-1.1.jar:dependencies/*","com.amazon.kinesis.streaming.agent.Agent"]  

