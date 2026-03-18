# PODIO → Apache Arrow Converter

Implementation for the **GSoC 2026 evaluation task: “Apache Arrow interface for PODIO”**.

This project provides a simple C++ executable that reads **EDM4hep EventHeader collections** from a ROOT file using **PODIO**, converts the data into **Apache Arrow columnar format**, and writes the result to a **Parquet file**.

---

# Overview

High-Energy Physics workflows often store event data in ROOT files using data models such as **EDM4hep**. While ROOT works well for traditional HEP analysis, columnar formats like **Apache Arrow / Parquet** enable better interoperability with modern data processing frameworks (Spark, Pandas, DuckDB, etc.).

This project demonstrates a minimal prototype for converting a subset of PODIO data into Arrow.

The program:

1. Reads an EDM4hep ROOT file using PODIO
2. Extracts the **EventHeader** collection
3. Converts the following fields to Arrow columns:

   * `eventNumber`
   * `runNumber`
   * `timeStamp`
   * `weight`
4. Writes the resulting Arrow table to a **Parquet file**

---

# Features

* CLI tool for converting ROOT → Parquet
* Uses **PODIO** for reading EDM4hep files
* Uses **Apache Arrow** for columnar data representation
* Outputs **Parquet** files
* Minimal dependency footprint

# Example Dataset

An example EDM4hep ROOT file can be downloaded with:

```
xrdcp root://dtn-eic.jlab.org//volatile/eic/EPIC/FULL/26.02.0/epic_craterlake/DIS/NC/10x100/minQ2=1/pythia8NCDIS_10x100_minQ2=1_beamEffects_xAngle=-0.025_hiDiv_1.0000.edm4hep.root .
```

---

# Build Instructions

After cloning the repo,

Create a build directory:

```
mkdir build
cd build
```

Configure with CMake:

```
cmake ..
```

Build the project:

```
make -j
```

---

# Usage

The program accepts two command line arguments:

```
./converter <input_root_file> <output_parquet_file>
```

Example:

```
./converter input.edm4hep.root output.parquet
```

---

# Output

The resulting Parquet file contains the following schema:

| Column      | Type  |
| ----------- | ----- |
| eventNumber | int32 |
| runNumber   | int32 |
| timeStamp   | int64 |
| weight      | float |

Each row corresponds to one event.

---

# Implementation Details

## Reading Data

PODIO's `ROOTReader` is used to iterate through events in the EDM4hep file and access the `EventHeader` collection.

## Arrow Conversion

Data from each event is appended to Arrow column builders:

* `arrow::Int32Builder`
* `arrow::Int64Builder`
* `arrow::FloatBuilder`

These builders are then combined into an Arrow `Table`.

## Parquet Writing

The Arrow table is written to disk using:

```
parquet::arrow::WriteTable(...)
```

---

# Repository Structure

```
.
├── CMakeLists.txt
├── src
│   └── converter.cpp
├── include
├── README.md
└── build
```

---

# Bonus Questions

## Alternative Arrow Schemas

Several schema designs could be considered:

### Flat Event Table (current implementation)

Each event corresponds to a row:

```
eventNumber | runNumber | timeStamp | weight
```

Advantages:

* Simple
* Easy to load into analytical tools

Limitations:

* Does not represent nested detector data.

---

### Nested Arrow Schema

Arrow supports nested structures such as:

* `Struct`
* `List`
* `Map`

Example:

```
event
 ├── header
 │    ├── eventNumber
 │    ├── runNumber
 │    └── timeStamp
 └── collections
```

This more closely mirrors the hierarchical structure of PODIO event data.

---

### Collection-per-table Model

Another approach is one table per collection:

```
EventHeader.parquet
MCParticles.parquet
Tracks.parquet
```

Events can then be linked via an event index.

Advantages:

* Better for large detector datasets
* Efficient columnar reads

---

# Supporting Arbitrary PODIO Data Models

To generalize this converter for **any PODIO data model**, the following approach could be used:

### 1. Runtime Schema Generation

Use PODIO metadata to dynamically discover:

* collection names
* field types
* relations

### 2. Automatic Arrow Schema Mapping

Map PODIO types to Arrow types:

| PODIO Type | Arrow Type |
| ---------- | ---------- |
| int        | int32      |
| float      | float      |
| double     | float64    |
| vector<T>  | List<T>    |

### 3. Reflection-Based Conversion

Iterate through fields dynamically instead of hardcoding EventHeader.

### 4. Schema Registry

Generate and store Arrow schemas for each PODIO collection for reuse.

---

# Future Improvements

* Support additional EDM4hep collections
* Parallel event conversion
* Zero-copy memory mapping
* Support nested Arrow schemas
* Streaming conversion for large datasets

---

# Technical Evolution: Handling Variable-Length VectorMembers
Supporting complex PODIO data models requires handling collections where the number of elements per event varies (e.g., Particle Weights, Hits, or Tracks). During this evaluation, I explored three distinct architectural approaches:

### 1. Approach: Standard Nested Lists
Initial tests used the traditional arrow::ListBuilder. While functional, this approach requires data to be strictly contiguous during ingestion, which doesn't fully align with the non-contiguous memory flexibility often found in PODIO's internal structures.

### 2. Approach: Optimized In-Memory ListView
I transitioned the ingestion phase to use arrow::ListView (introduced in Arrow 14.0+).

The Advantage: ListView is technically superior for HEP data because it allows for non-contiguous and overlapping memory slices in RAM.

The Hurdle: I identified an upstream limitation in the C++ Parquet writer (Ref: Arrow GH-9344). Currently, the C++ implementation of Parquet cannot directly serialize ListView because the Parquet specification requires a strictly sequential, 3-level nested list structure.

### 3. Approach: Hybrid Ingestion with Buffer Normalization (Final)
To maintain high performance in-memory while ensuring 100% compatibility, I implemented a Buffer Normalization Layer.

Ingestion: Data is ingested into a ListViewBuilder, the ideal internal format for a PODIO-Arrow engine.

Normalization: Before serialization, a normalization step manually extracts the offsets and lengths from the ListView and performs a high-speed sequential copy into a standard ListArray.

Serialization: These normalized buffers satisfy the strict invariants required by parquet-cpp, ensuring the file is readable by any standard Parquet consumer.