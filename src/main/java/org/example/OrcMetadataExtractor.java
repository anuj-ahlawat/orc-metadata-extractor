package org.example;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.StripeInformation;
import org.apache.orc.StripeStatistics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OrcMetadataExtractor {

    public static class ColumnInfo {
        public String column;
        public long nulls;
        public long value_count;
        public String min;
        public String max;
    }

    public static class StripeInfo {
        public int Stripe;
        public long Row_count;
        public List<ColumnInfo> columns;
    }

    public static class FileInfo {
        public String File;
        public String Compression;
        public long Number_of_rows;
        public String Column_types;
        public int Number_of_stripes;
        public List<StripeInfo> Stripes;
    }

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("ORC Metadata Extractor").master("local[*]").getOrCreate();

        String folderPath = "stores_orc";
        String outputPath = "./output/orc_metadata_output.json";

        File folder = new File(folderPath);
        File outputDir = new File(outputPath).getParentFile();
        if (!outputDir.exists()) outputDir.mkdirs();

        List<FileInfo> allFileMetadata = new ArrayList<>();
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".orc"));
        if (files == null) return;

        for (File orcFile : files) {
            String filePath = orcFile.getAbsolutePath();
            Dataset<Row> df = spark.read().orc(filePath);
            StructField[] fields = df.schema().fields();

            FileInfo fileInfo = new FileInfo();
            fileInfo.File = orcFile.getName();

            // Compression, Number of rows, and Stripe Info using ORC native
            Configuration conf = new Configuration();
            Reader reader = OrcFile.createReader(new Path(filePath), OrcFile.readerOptions(conf));
            fileInfo.Compression = reader.getCompressionKind().name();
            fileInfo.Number_of_rows = reader.getNumberOfRows();
            fileInfo.Number_of_stripes = reader.getStripes().size();

            // Column types
            StringBuilder schemaString = new StringBuilder("struct<");
            for (int j = 0; j < fields.length; j++) {
                schemaString.append(fields[j].name()).append(":").append(fields[j].dataType().simpleString());
                if (j != fields.length - 1) schemaString.append(",");
            }
            schemaString.append(">");
            fileInfo.Column_types = schemaString.toString();

            List<StripeInfo> stripeInfoList = new ArrayList<>();

            List<StripeInformation> stripes = reader.getStripes();
            for (int i = 0; i < stripes.size(); i++) {
                StripeInformation stripe = stripes.get(i);
                StripeInfo stripeInfo = new StripeInfo();
                stripeInfo.Stripe = i;
                stripeInfo.Row_count = stripe.getNumberOfRows();
                stripeInfo.columns = new ArrayList<>();

                StripeStatistics stripeStats = reader.getStripeStatistics().get(i);
                ColumnStatistics[] colStats = stripeStats.getColumnStatistics();

                for (int j = 1; j < colStats.length; j++) { 
                    ColumnInfo columnInfo = new ColumnInfo();
                    String colName = fields[j - 1].name();
                    columnInfo.column = colName;
                    columnInfo.nulls = colStats[j].getNumberOfValues();
                    columnInfo.value_count = stripeInfo.Row_count - columnInfo.nulls;

                    // Min/max
                    if (colStats[j] instanceof IntegerColumnStatistics) {
                        IntegerColumnStatistics intStat = (IntegerColumnStatistics) colStats[j];
                        columnInfo.min = String.valueOf(intStat.getMinimum());
                        columnInfo.max = String.valueOf(intStat.getMaximum());
                    } else if (colStats[j] instanceof DoubleColumnStatistics) {
                        DoubleColumnStatistics dblStat = (DoubleColumnStatistics) colStats[j];
                        columnInfo.min = String.valueOf(dblStat.getMinimum());
                        columnInfo.max = String.valueOf(dblStat.getMaximum());
                    } else if (colStats[j] instanceof StringColumnStatistics) {
                        StringColumnStatistics strStat = (StringColumnStatistics) colStats[j];
                        columnInfo.min = strStat.getMinimum();
                        columnInfo.max = strStat.getMaximum();
                    } else {
                        columnInfo.min = null;
                        columnInfo.max = null;
                    }
                    stripeInfo.columns.add(columnInfo);
                }
                stripeInfoList.add(stripeInfo);
            }

            fileInfo.Stripes = stripeInfoList;
            allFileMetadata.add(fileInfo);

            // Write individual JSON
            FileWriter fw = new FileWriter(new File(outputDir, orcFile.getName().replace(".orc", ".json")));
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(fw, fileInfo);
            fw.close();
        }

        // Write all files metadata JSON to a single file
        FileWriter fwAll = new FileWriter(outputPath);
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(fwAll, allFileMetadata);
        fwAll.close();

        spark.stop();
    }
}
