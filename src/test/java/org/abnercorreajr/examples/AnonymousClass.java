package org.abnercorreajr.examples;

import com.google.common.reflect.TypeToken;
import org.abnercorreajr.util.ReflectUtil;


public class AnonymousClass {

    static abstract class IKnowMyType<T> {
        TypeToken<T> type = new TypeToken<>(getClass()) {};

        public Class<T> getGenericArgClass()  {
            try {
                return (Class<T>) Class.forName(type.getType().getTypeName());
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        public T genericInstance() {
            return ReflectUtil.newInstance(getGenericArgClass());
        }
    }

    static abstract class A { public String x; }
    static class AA extends A { public String y; }

    interface B {}

    public static void main(String[] args) {
        A a = new A() { public String y; };
        AA aa = new AA();

        a.x = "";
        aa.x = "";

        //a.y = "";
        aa.y = "";

        //B b = new B();
    }







//        String str = new IKnowMyType<String>() {}.genericInstance();
//        System.out.println(str.isBlank());

}
