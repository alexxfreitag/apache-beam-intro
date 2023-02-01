package org.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {

        MyOption myOption = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOption.class);
        myOption.setTempLocation("gs://apache-beam-training-dev/input");
        myOption.setStagingLocation("gs://apache-beam-training-dev/input");
        myOption.setProject("gcs-flow-big");

        Pipeline p = Pipeline.create(myOption);

        List<TableFieldSchema> columns = new ArrayList<TableFieldSchema>();
        columns.add(new TableFieldSchema().setName("first_name").setType("STRING"));
        columns.add(new TableFieldSchema().setName("last_name").setType("STRING"));
        columns.add(new TableFieldSchema().setName("company_name").setType("STRING"));
        columns.add(new TableFieldSchema().setName("address").setType("STRING"));
        columns.add(new TableFieldSchema().setName("city").setType("STRING"));
        columns.add(new TableFieldSchema().setName("country").setType("STRING"));
        columns.add(new TableFieldSchema().setName("state").setType("STRING"));
        columns.add(new TableFieldSchema().setName("zip").setType("STRING"));
        columns.add(new TableFieldSchema().setName("phone1").setType("STRING"));
        columns.add(new TableFieldSchema().setName("phone2").setType("STRING"));
        columns.add(new TableFieldSchema().setName("email").setType("STRING"));
        columns.add(new TableFieldSchema().setName("web").setType("STRING"));

        TableSchema tblSchema = new TableSchema().setFields(columns);


        PCollection<String> pInput = p.apply(TextIO.read().from("gs://apache-beam-training-dev/input/user.csv"));


        pInput.apply(ParDo.of(new DoFn<String, TableRow>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String arr[] = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                        TableRow row = new TableRow();
                        row.set("first_name", arr[0]);
                        row.set("last_name", arr[1]);
                        row.set("company_name", arr[2]);
                        row.set("address", arr[3]);
                        row.set("city", arr[4]);
                        row.set("country", arr[5]);
                        row.set("state", arr[6]);
                        row.set("zip", arr[7]);
                        row.set("phone1", arr[8]);
                        row.set("phone2", arr[9]);
                        row.set("email", arr[10]);
                        row.set("web", arr[11]);
                        c.output(row);

                    }
                }))
                .apply(BigQueryIO.writeTableRows().to("apache_beam.user")
                        .withSchema(tblSchema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                );

        p.run().waitUntilFinish();

    }

    /*public static void main(String[] args) {
        Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        p.apply(Create.of("Hello", "World"))
                .apply(MapElements.via(new SimpleFunction<String, String>() {
                    @Override
                    public String apply(String input) {
                        return input.toUpperCase();
                    }
                }))
                .apply(ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        System.out.println(c.element());
                    }
                }));

        p.run();
    }*/
}


/* Pipeline p = Pipeline.create(
                PipelineOptionsFactory.fromArgs(args).withValidation().create()
        );

        PCollection<String> pInput = p.apply(TextIO.read().from("gs://apache-beam-training-dev/input/user.csv"));

        pInput.apply(ParDo.of(new DoFn<String, Void>() {

            @ProcessElement
            public void processElement(ProcessContext c) {
                System.out.println(c.element());
            }
        }));

        p.run();
*/