FROM java:8
WORKDIR /
ADD target/protograph-0.0.20-standalone.jar protograph.jar
