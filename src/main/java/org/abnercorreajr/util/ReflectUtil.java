package org.abnercorreajr.util;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

public class ReflectUtil {

    @SuppressWarnings("unchecked")
    public static <T> Class<T> getInterfaceGenericArgClass(Class<?> child, Class<?> interfaceClass) {
        String interfaceTypeName = interfaceClass.getTypeName().replaceAll("<.*>", "");

        // Ah, those Scala days...
        return Arrays
            .stream(child.getGenericInterfaces())
            .filter(t -> interfaceTypeName.equals(t.getTypeName().replaceAll("<.*>", "")))
            .findFirst()
            .map(type -> (Class<T>) ((ParameterizedType) type).getActualTypeArguments()[0])
            .orElse(null);
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> classFor(TypeToken<T> type) {
        return (Class<T>) type.getRawType();
    }

    @SuppressWarnings("unchecked")
    public static <T> Class<T> classFor(String className) {
        try {
            return (Class<T>) Class.forName(className);
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T newInstance(Class<T> klass, Object... initargs) {
        try {
            return klass.getDeclaredConstructor().newInstance(initargs);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
