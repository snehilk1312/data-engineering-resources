FROM openjdk
EXPOSE 8080
WORKDIR /usr/src/app
copy . .
RUN ./mvnw package
CMD ["java","-jar","./target/docker-example-1.1.3.jar"]


