FROM java:8
WORKDIR /
ADD target/protograph-0.0.13-standalone.jar protograph.jar
