/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */



package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.util.DurationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;

/**
 * This pipeline ingests incoming data from a Cloud Pub/Sub topic and
 * outputs the raw data into windowed files at the specified output
 * directory.
 *
 * <p> Example Usage:
 *
 * <pre>
 * mvn compile exec:java \
 -Dexec.mainClass=org.apache.beam.examples.templates.${PIPELINE_NAME} \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=${PROJECT_ID} \
 --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 --runner=DataflowRunner \
 --windowDuration=2m \
 --numShards=1 \
 --topic=projects/${PROJECT_ID}/topics/windowed-files \
 --outputDirectory=gs://${PROJECT_ID}/temp/ \
 --outputFilenamePrefix=windowed-file \
 --outputFilenameSuffix=.txt"
 * </pre>
 * </p>
 */
public class PubsubToText {


  /** The Google Project ID. */
  private static final String PROJECT_ID = "creative-analytics";

  /** The GCS bucket to insert records into. */
  private static final String BUCKET_NAME = "adevents.playground.xyz";

  /** The PXYZ Event types to subscribe to. */
  private static final String[] EVENT_TYPES = {
    "clickthrough",
    "dismissal",
    "engagement",
    "event",
    "expand",
    "impression",
    "milestone",
    "render",
    "request",
    "survey",
    "survey-render",
    "survey-request",
    "survey-reset",
    "survey-response",
    "survey-viewable",
    "video-fullscreen",
    "video-mute",
    "video-play",
    "video-progress",
    "video-viewable",
    "viewable"
  };

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.</p>
   */
  public interface Options extends PipelineOptions, StreamingOptions {

    @Description("The window duration in which data will be written. Defaults to 5m. "
        + "Allowed formats are: "
        + "Ns (for seconds, example: 5s), "
        + "Nm (for minutes, example: 12m), "
        + "Nh (for hours, example: 2h).")
    @Default.String("5m")
    String getWindowDuration();
    void setWindowDuration(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   * @param args  The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory
        .fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return  The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /* Attach a listener for each event type to the pipeline */
    for (int i = 0; i < EVENT_TYPES.length; i++) {
      attachEventType(
          pipeline,
          EVENT_TYPES[i],
          options
      );
    }

    return pipeline.run();
  }

  public static Pipeline attachEventType(Pipeline pipeline, String eventType, Options options) {
    String outputFolder = "gs://" + BUCKET_NAME + "/backup/" + eventType + "/";
    /*
     * Steps:
     *   1) Read string messages from PubSub
     *   2) Window the messages into minute intervals specified by the executor.
     *   3) Output the windowed files to GCS
     */
    pipeline
        .apply("Read PubSub Events/" + eventType, PubsubIO.readStrings()
              .fromTopic("projects/" + PROJECT_ID + "/topics/" + eventType))
        .apply(
            options.getWindowDuration() + " Window/" + eventType,
            Window.into(FixedWindows.of(DurationUtils.parseDuration(options.getWindowDuration())))
        )

        // Apply windowed file writes. Use a NestedValueProvider because the filename
        // policy requires a resourceId generated from the input value at runtime.
        .apply(
            "Write File(s)/" + eventType,
            TextIO.write()
                .withWindowedWrites()
                .withNumShards(1)
                .to(new WindowedFilenamePolicy(
                    StaticValueProvider.of(outputFolder),
                    StaticValueProvider.of(""),
                    StaticValueProvider.of("W"),
                    StaticValueProvider.of("")
                ))
                .withTempDirectory(NestedValueProvider.of(
                    StaticValueProvider.of(outputFolder),
                    (SerializableFunction<String, ResourceId>) input ->
                        FileBasedSink.convertToFileResourceIfPossible(input))));

    return pipeline;
  }
}
