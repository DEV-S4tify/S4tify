#! /bin/bash
java -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx8G -jar target/scala-2.12/eventsim-assembly-1.0.jar $*