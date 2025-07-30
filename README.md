# ORC Spark Metadata Extractor

This project is a simple Java application that uses Apache Spark to extract metadata from ORC files in a folder. It outputs the metadata as JSON files for each ORC file and a combined JSON summary.

## Motivation
Working with large datasets stored in ORC (Optimized Row Columnar) format is common in big data environments. Sometimes, you need to quickly understand what data is inside these files—such as the number of rows, what columns exist, and some basic statistics—without loading them into a database or writing complex queries. This tool helps you get that information easily and in a human-readable format.

## Features
- Reads all `.orc` files from a specified folder
- Extracts row count, column types, null/value counts, and min/max for numeric columns
- Outputs metadata as pretty-printed JSON files (one per ORC file and one summary file)
- Easy to use and modify for your own needs

## Folder Structure
```
orc_spark_metadata_extractor/
├── pom.xml
├── README.md
├── src/
│   └── main/
│       └── java/
│           └── org/
│               └── example/
│                   └── OrcMetadataExtractor.java
├── stores_orc/
│   └── (your .orc files here)
└── output/
    ├── orc_metadata_output.json
    └── (individual file metadata .json)
```

## What Metadata is Extracted?
For each ORC file, the following information is collected:
- **File**: The name of the ORC file
- **Compression**: Detected from the file name (e.g., SNAPPY, ZLIB, or UNKNOWN)
- **Number of rows**: Total number of rows in the file
- **Column types**: The schema of the file, showing each column's name and data type
- **Number of stripes**: Always 1 in this tool (for simplicity)
- **Stripes**: For each stripe (here, just one), includes:
  - **Stripe**: Stripe number (always 0)
  - **Row count**: Number of rows in the stripe
  - **columns**: For each column:
    - **column**: Name of the column
    - **nulls**: Number of null values
    - **value_count**: Number of non-null values
    - **min**: Minimum value (for numeric columns)
    - **max**: Maximum value (for numeric columns)

## Requirements
- Java 17 (or compatible with your Spark version)
- Apache Spark 3.x or 4.x
- Maven
- ORC files in a folder (default: `stores_orc`)

## Setup
1. Clone this repository:
   ```sh
   git clone https://github.com/yourusername/orc-spark-metadata-extractor.git
   cd orc-spark-metadata-extractor
   ```
2. Place your ORC files in the `stores_orc` folder (create it if it doesn't exist).
3. Make sure you have Java and Spark installed and available in your PATH.

## Build
Compile the project using Maven:
```sh
mvn clean package
```

## Run
Run the extractor using Spark:
```sh
spark-submit \
  --class org.example.OrcMetadataExtractor \
  --master "local[*]" \
  target/orc-spark-metadata-extractor-1.0-SNAPSHOT.jar
```

## Output
- Individual JSON files for each ORC file will be created in the `output` folder.
- A combined summary file will be created at `output/orc_metadata_output.json`.

## Example
After running, you will see files like:
- `output/part-00000-xxxx.json`
- `output/orc_metadata_output.json`

Each JSON contains metadata such as:
- File name
- Compression type
- Number of rows
- Column types
- Stripe and column statistics (nulls, value counts, min/max for numbers)

## Customizing Input and Output
- **Input folder**: By default, the program looks for ORC files in the `stores_orc` folder. You can change this by editing the `folderPath` variable in `OrcMetadataExtractor.java`.
- **Output location**: The summary JSON is written to `output/orc_metadata_output.json`. You can change this by editing the `outputPath` variable in the code.

## Troubleshooting
- **Java or Spark version errors**: Make sure you are using a Java version compatible with your Spark installation (Java 17 is recommended for Spark 3.x/4.x).
- **No output files**: Check that your `stores_orc` folder contains valid `.orc` files.
- **Permission errors**: Make sure you have write permissions to the `output` folder.
- **Out of memory**: For very large ORC files, you may need to increase Spark's memory settings.

## License
MIT 