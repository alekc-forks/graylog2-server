FROM graylog/graylog:3.3.1
#COPY graylog2-server/target/graylog.jar /usr/share/graylog/graylog.jar
COPY  graylog2-server/target/graylog2-server-3.3.1-SNAPSHOT-shaded.jar /usr/share/graylog/graylog.jar
