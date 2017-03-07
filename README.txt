This is an implementation of Ricart and Agarwala's Mutual exclusion algorithm.

Compile:
javac MyProg.java Request.java RequestImpl.java RequestMessageProtos.java

Run N nodes:
java -classpath . -Djava.rmi.server.codebase=file:./ MyProg <N> <output_file> <current_node_id_starting_from_1>

Prerequisites
com.google.protobuf package in current folder.
Running RMIregistry in which the nodes register.
