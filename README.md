# Scio Quickstart

This repository contains a sample pipeline for getting started with [Scio](https://spotify.github.io/scio/), the Scala framework for [Apache Beam](https://beam.apache.org/) pipelines.

Fork or clone this repository so you can commit your changes in your own repository.

## Repository Branches

This repository is organized into two main branches:

* **`main`**: The default branch. It contains the exercise skeleton code with incomplete pipeline logic (`runPipeline` method set to `???`). Use this branch to implement your own solution to the exercise.
* **`solution`**: Contains the full reference implementation and solution. You can switch to this branch at any time to compare your progress or check the working pipeline:
  ```bash
  git checkout solution
  ```

---

## Prerequisites

To compile and run this project, ensure you have the following installed:

* **Java JDK**: JDK 17 or higher.
* **SBT**: Scala Build Tool (v1.x or higher). See [installation instructions](https://www.scala-sbt.org/).

---

## Exercise: Sancho vs. Dulcinea

The goal of this example pipeline is to process and analyze the text of *Don Quixote*, the famous novel by Miguel de Cervantes. The novel features several prominent characters, including Sancho Panza (Don Quixote's squire) and Dulcinea del Toboso (his romantic ideal).

The pipeline should:
1. Read the input text files.
2. Clean up punctuation, normalize casing, and tokenize words.
3. Count word occurrences and sort them in descending order.
4. Answer the existential question: **Who is mentioned more frequently in the novel, Sancho or Dulcinea?**

---

## Input Data

The `data/` directory contains two text datasets:

* `muestra.txt`: A small extract of the novel, ideal for quick testing and debugging during pipeline development.
* `el_quijote.txt`: The full text of the novel, used for final analysis to solve the Sancho vs. Dulcinea comparison.

---

## Development & Compilation

You can compile and build the repository using SBT:

* **Compile the project**:
  ```bash
  sbt compile
  ```
* **Run directly via SBT**:
  ```bash
  sbt "run --input-file=./data/muestra.txt --output-file=tmp --num-words=10"
  ```
* **Launch interactive Scio REPL**:
  ```bash
  sbt repl/run
  ```
* **Package / Stage executable**:
  ```bash
  sbt stage
  ```

---

## Running the Staged Executable

Once you run `sbt stage`, an executable launcher script is generated under `target/universal/stage/bin/scio-quickstart`.

### 1. Test run on sample data

Find the top 10 words in the sample dataset:
```bash
./target/universal/stage/bin/scio-quickstart --input-file=./data/muestra.txt --output-file=tmp --num-words=10
```

The output will be written to text files inside the `tmp/` directory (e.g., `tmp/part-00000-of-00001.txt`).

### 2. Full run on complete novel

Process the full novel and retrieve the top 100 words:
```bash
./target/universal/stage/bin/scio-quickstart --input-file=./data/el_quijote.txt --output-file=tmp --num-words=100
```

Inspect the output file in `tmp/` to check the word counts for `sancho` and `dulcinea` and solve the mystery!