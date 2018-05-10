FROM java:8
WORKDIR /
ADD target/protograph-0.0.22-standalone.jar protograph.jar
