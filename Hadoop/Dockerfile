FROM openjdk:11
VOLUME /tmp
ADD src/main/java/target/*.jar app.jar
EXPOSE 8888
ENTRYPOINT ["java","-jar","app.jar"]