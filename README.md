# Scio quickstart

This repository contains a sample pipeline for starting with [Scio](https://spotify.github.io/scio/), the Scala 
framework to develop Apache Beam pipelines.

# Pipeline

The goal of this example is to count the words in Don Quixote, the famous novel by Miguel de Cervantes. The novel has
several characters: Sancho, the buddy of Don Quixote; Dulcinea, the significant other of Don Quixote; Rocinante, the
fearful horse of Don Quixote, etc. 

The pipeline does not only count the words, it also sorts the words by number of occurrences, and provides an answer 
to an existential question: who is mentioned more in the novel, Sancho or Dulcinea?

Let's find out with the help of Scio.

## Compile

The first step to solve the mysterious question is to compile the code. For that, you will need to have installed SBT:
* https://www.scala-sbt.org/

When you have installed, you can run

* `sbt compile`  to compile the code (for instance, while you are developing the code for the pipeline)
* `sbt stage` to produce a runnable package

## Input data

In the `data` directory you will find two files:

* `sample.txt`, small extract of the novel. You can use this for tests while you are developing the pipeline
* `el_quijote.txt`, the full novel, to solve the important question about Sancho or Dulcinea

## Running the example

Once you have run `sbt stage`, there will be a script in the directory `target/universal/stage/bin`. You can use that
script to run the pipeline.

For instance, to find the top 10 words in the sample data:

`./target/universal/stage/bin/scio-quickstart --input-file=./data/sample.txt --output-file=tmp --num-words=10`

After that you should find a file with a name like ` part-00000-of-00001.txt` in the `tmp` subdirectory.

To run with the full data and top 100 words:

`./target/universal/stage/bin/scio-quickstart --input-file=./data/el_quijote.txt --output-file=tmp --num-words=100`

Search for `sancho` and `dulcinea` in the output to solve this burning question.
