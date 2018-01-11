FROM java:8
WORKDIR /
ADD target/protograph-0.0.16-standalone.jar protograph.jar
