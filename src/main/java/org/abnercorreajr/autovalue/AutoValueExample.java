/*
 https://github.com/google/auto/blob/main/value/userguide/index.md

 What's going on here?

 AutoValue runs inside javac as a standard annotation processor.
 It reads your abstract class and infers what the implementation class should look like.
 It generates source code, in your package, of a concrete implementation class which extends your abstract class, having:

    - package visibility (non-public)
    - one field for each of your abstract accessor methods
    - a constructor that sets these fields
    - a concrete implementation of each accessor method returning the associated field value
    - an equals implementation that compares these values in the usual way
    - an appropriate corresponding hashCode
    - a toString implementation returning a useful (but unspecified) string representation of the instance

 Your hand-written code, as shown above, delegates its factory method call to the generated constructor and voilà!

 For the Animal example shown above, here is typical code AutoValue might generate.

 Note that consumers of your value class don't need to know any of this.
 They just invoke your provided factory method and get a well-behaved instance back.

 if you are using a version of Java that has records, then those are usually more appropriate.
 For a detailed comparison of AutoValue and records, including information on how to migrate from one to the other, see here.
 */

package org.abnercorreajr.autovalue;

import com.google.auto.value.AutoValue;

import javax.annotation.Nullable;

@AutoValue
public abstract class AutoValueExample {

    abstract Integer getA();

    abstract @Nullable Long getB();

    abstract Builder toBuilder();

    public static Builder builder() {
        return new AutoValue_AutoValueExample.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
        // Builder must have a single no-argument method, typically called build(), that returns org.abnercorreajr.examples.AutoValueExample
        abstract AutoValueExample build();

        abstract Builder setA(Integer a);

        abstract Builder setB(Long b);
    }
}
