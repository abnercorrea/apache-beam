package org.abnercorreajr.beam.fn;

import org.apache.beam.sdk.transforms.Combine;

public class MinFn {

    public static MinStringFn ofStrings() {
        return new MinStringFn();
    }

    public static class MinStringFn extends Combine.BinaryCombineFn<String> {
        @Override
        public String apply(String left, String right) {
            return (left != null ? left.compareTo(right) : 0) <= 0 ? left : right;
        }
    }

}
