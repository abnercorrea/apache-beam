package org.abnercorreajr.util;

import java.util.function.Supplier;

public class OrElse {
    public static <T> T get(T value, T defaultValue) {
        return (value == null) ? defaultValue : value;
    }

    public static <T> T get(T value, Supplier<? extends T> defaultF) {
        return (value == null) ? defaultF.get() : value;
    }
}
