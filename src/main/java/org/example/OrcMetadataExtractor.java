package org.example;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OrcMetadataExtractor {
    public static class ColumnInfo {
        public String column; public long nulls, value_count; public String min, max;
    }
    public static class StripeInfo {
        public int Stripe; public long Row_count; public List<ColumnInfo> columns;
    }
    public static class FileInfo {
        public String File, Compression, Column_types; public long Number_of_rows; public int Number_of_stripes; public List<StripeInfo> Stripes;
    }
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("ORC Metadata Extractor").master("local[*]").getOrCreate();
        String folderPath = "stores_orc";
        String outputPath = "./output/orc_metadata_output.json";
        File folder = new File(folderPath);
        File outputDir = new File(outputPath).getParentFile();
        if (!outputDir.exists()) outputDir.mkdirs();
        List<FileInfo> allFiles = new ArrayList<>();
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (!file.getName().endsWith(".orc")) continue;
            Dataset<Row> df = spark.read().orc(file.getAbsolutePath());
            FileInfo fi = new FileInfo();
            fi.File = file.getName();
            fi.Compression = getCompression(file.getName());
            fi.Number_of_rows = df.count();
            fi.Column_types = getColTypes(df);
            fi.Number_of_stripes = 1;
            StripeInfo stripe = new StripeInfo();
            stripe.Stripe = 0;
            stripe.Row_count = fi.Number_of_rows;
            stripe.columns = new ArrayList<>();
            for (StructField f : df.schema().fields()) {
                ColumnInfo c = new ColumnInfo();
                c.column = f.name();
                c.nulls = df.filter(df.col(f.name()).isNull()).count();
                c.value_count = df.filter(df.col(f.name()).isNotNull()).count();
                String t = f.dataType().simpleString();
                if (t.equals("int") || t.equals("bigint") || t.equals("float") || t.equals("double") || t.equals("long")) {
                    Object min = df.agg(functions.min(f.name())).first().get(0);
                    Object max = df.agg(functions.max(f.name())).first().get(0);
                    c.min = min != null ? min.toString() : null;
                    c.max = max != null ? max.toString() : null;
                } else { c.min = null; c.max = null; }
                stripe.columns.add(c);
            }
            fi.Stripes = Collections.singletonList(stripe);
            allFiles.add(fi);
            try (FileWriter fw = new FileWriter(new File(outputDir, file.getName().replace(".orc", ".json")))) {
                new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(fw, fi);
            }
        }
        try (FileWriter fw = new FileWriter(outputPath)) {
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(fw, allFiles);
        }
        spark.stop();
    }
    private static String getColTypes(Dataset<Row> df) {
        StringBuilder sb = new StringBuilder("struct<");
        StructField[] f = df.schema().fields();
        for (int i = 0; i < f.length; i++) {
            sb.append(f[i].name()).append(":").append(f[i].dataType().simpleString());
            if (i != f.length - 1) sb.append(",");
        }
        return sb.append(">").toString();
    }
    private static String getCompression(String filename) {
        filename = filename.toLowerCase();
        if (filename.contains("snappy")) return "SNAPPY";
        if (filename.contains("zlib")) return "ZLIB";
        return "UNKNOWN";
    }
}