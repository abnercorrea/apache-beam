package org.abnercorreajr.beam.examples;

import org.abnercorreajr.beam.pipelines.PipelineBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.*;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;
import java.util.Map;

public class PrintPipeline {

    @SuppressWarnings("NullableProblems")
    static class PrintPipelineVisitor extends Pipeline.PipelineVisitor.Defaults {

        @Override
        public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            System.out.print(node.isRootNode() ? "ROOT" : node.getFullName());

            if (node.getTransform() != null) {
                System.out.print("\n\tTransform: " + node.getTransform());
            }
            System.out.print("\n\tInputs: " + node.getInputs());
            System.out.println("\n\tOutputs: " + node.getOutputs());

            return CompositeBehavior.ENTER_TRANSFORM;
        }

        @Override
        public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            System.out.print("PRIMITIVE: " + node.getFullName());
            System.out.print("\n\tInputs: " + node.getInputs());
            System.out.print("\n\tOutputs: " + node.getOutputs());
            System.out.println("\n\tTransform: " + node.getTransform());
        }
    }

    @SuppressWarnings("NullableProblems")
    static class SourceFinderVisitor extends Pipeline.PipelineVisitor.Defaults {

        public TransformHierarchy.Node root = null;
        public TransformHierarchy.Node source = null;


        @Override
        public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
            if (node.isRootNode()) {
                root = node;
            }
            else if (node.getInputs().isEmpty() && source == null) {
                source = node;
            }

            return CompositeBehavior.ENTER_TRANSFORM;
        }

        @Override
        public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (node.getInputs().isEmpty() && source == null) {
                source = node;
            }
        }
    }
    public static void main(String[] args) {
        Pipeline pipeline = PipelineBuilder.of(LogElementsExample.class).build();
        //Pipeline pipeline = Pipeline.create(); pipeline.apply(Impulse.create());
        //Pipeline pipeline = Pipeline.create(); pipeline.apply(GenerateSequence.from(0).to(10));

        SourceFinderVisitor visitor = new SourceFinderVisitor();
        pipeline.traverseTopologically(visitor);

        System.out.println(visitor.source.getFullName());
        System.out.println(visitor.source.isCompositeNode());

        new LogElementsExample().run(args);
   }
}
