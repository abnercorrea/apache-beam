# Repo with Apache Beam Java code.

Interrresting things:

1. Transform to manipulate Rows and update the schema automatically. Example:
```java
pcoll.apply(
    TransformRows.create()
        .withField("a", TypeDescriptors.longs(), r -> r.getInt64("a"))
        .withField("b", TypeDescriptors.strings(), r -> r.getString("b") + " " + r.getInt64("a"))
        .withField("aIsEven", TypeDescriptors.booleans(), r -> r.getInt64("a") % 2 == 0)
        .withField("c", TypeDescriptors.longs(), generateC)
        .withField("d", TypeDescriptors.longs(), r -> 2 * generateC.apply(r))
        .withField("numbers", TypeDescriptors.lists(TypeDescriptors.longs()), r -> List.of(r.getInt64("key"), r.getInt64("a")))
        .withFieldRenamed("b", "name")
        .withFieldRenamed("d", "doubleC")
        .drop("key")
)
```

2. Simple framework to unit test beam pipelines as is, without rewriting them to replace / remove sources and sinks. Example:

- Pipeline:
[src/main/java/org/abnercorreajr/beam/examples/BigQueryExample.java](https://github.com/abnercorrea/apache-beam/blob/97252aaf55d94aaec249b07763dfe1658d2a6c68/src/main/java/org/abnercorreajr/beam/examples/BigQueryExample.java)
- Test Case:
[src/test/java/org/abnercorreajr/beam/examples/BigQueryExampleTest.java](https://github.com/abnercorrea/apache-beam/blob/97252aaf55d94aaec249b07763dfe1658d2a6c68/src/test/java/org/abnercorreajr/beam/examples/BigQueryExampleTest.java)
