FROM openjdk:11
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get update 
RUN apt-get -y install sbt=1.2.8
WORKDIR /opt/eventsim
COPY . /opt/eventsim
RUN sbt assembly
RUN chmod +x /opt/eventsim/eventsim.sh
ENTRYPOINT ["/opt/eventsim/eventsim.sh"]