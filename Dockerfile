FROM java:8
WORKDIR /
ADD target/protograph-0.0.17-standalone.jar protograph.jar
