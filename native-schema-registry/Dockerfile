#FROM ghcr.io/graalvm/native-image:22.0.0.2
#RUN microdnf install tar gzip
#WORKDIR /aws-glue-scham-registry
#COPY ./ .
#WORKDIR native-schema-registry
#
## download and install Maven
#ARG MAVEN_VERSION=3.8.6
#RUN mkdir /usr/share/maven \
#    && curl -fsSL -o /tmp/apache-maven.tar.gz \
#    https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
#    && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
#    && rm -f /tmp/apache-maven.tar.gz \
#    && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn
#
#ENV MAVEN_HOME /usr/share/maven
#ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"

FROM maven AS base_builder

ARG GRAAL_VERSION=22.0.0.2

# install graalvm
RUN mkdir /usr/share/graal \
    && curl -fsSL -o /tmp/graalvm.tar.gz  \
    https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-22.0.0.2/graalvm-ce-java11-linux-amd64-22.0.0.2.tar.gz \
    && tar -xzf /tmp/graalvm.tar.gz -C /usr/share/graal --strip-components=1 \
    && rm -f /tmp/cmake.tar.gz

ENV JAVA_HOME=/usr/share/graal
ENV PATH=/usr/share/graal/bin:$PATH

RUN gu install native-image
RUN apt update
RUN apt install -y cmake build-essential swig python3-dev lcov clang-tidy snapd

WORKDIR /aws-glue-scham-registry
COPY ./ .
RUN mvn clean install
WORKDIR native-schema-registry/c
RUN rm -rf build
RUN cmake -S. -Bbuild
WORKDIR build
RUN cmake --build . --target native_schema_registry_c_data_types
WORKDIR /aws-glue-scham-registry/native-schema-registry
RUN mvn package -P native-image
WORKDIR c/build
RUN cmake --build .

FROM amazonlinux:2 AS dontnet_builder

RUN rpm -Uvh https://packages.microsoft.com/config/centos/7/packages-microsoft-prod.rpm
RUN yum update -y
RUN yum install -y dotnet-sdk-3.1 tar gzip

COPY --from=base_builder /aws-glue-scham-registry /aws-glue-scham-registry



