# Native Schema Registry 

This module provides a native shared library (.so, .dll) version of the AWS Glue Schema Registry SerDes. 
It uses GraalVM to generate the shared library.

Note: this currently only builds in Linux.

## Setup

###Prerequisites

1. Install cmake and maven
2. sudo yum install libz-devel, python3-devel (need confirmation)
3. sudo pip3 install auditwheel, patchelf (may need to update pip3 beforehand)
4. Install GraalVM (java11 - amd): https://www.graalvm.org/
5. Set JAVA_HOME to use GraalVM
```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/<graalvm>/Contents/Home
```
6. Install the native image with gu:
```asm
gu install native-image
```
7. Build the C project data types, following the steps in the README file in C directory
8. Then run mvn install with the native image in the native-schema-registry directory:
```asm
mvn package -P native-image
```
9. Install swig

###Build
1. Create a python venv (may need to use python3.7)
2. Build the C project - run the following in c/build
```asm
cmake --build .
```
3. run the install script in python folder
```asm
bash install.sh <your_python_version>
```

