package org.abnercorreajr.beam.fn;

import org.apache.beam.sdk.transforms.Combine;

public class MaxFn {

    public static MaxStringFn ofStrings() {
        return new MaxStringFn();
    }

    public static class MaxStringFn extends Combine.BinaryCombineFn<String> {
        @Override
        public String apply(String left, String right) {
            return (left != null ? left.compareTo(right) : 0) >= 0 ? left : right;
        }
    }

}
