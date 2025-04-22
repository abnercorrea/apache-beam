package org.abnercorreajr.examples;

import com.google.common.reflect.TypeToken;
import org.abnercorreajr.util.ReflectUtil;

public class TypeTokenExample {

    static class IKnowMyType<T, V> {
        public Class<T> getGenericTypeClassT()  {
            return ReflectUtil.classFor(new TypeToken<T>(getClass()) {});
        }

        public Class<V> getGenericTypeClassV()  {
            return ReflectUtil.classFor(new TypeToken<V>(getClass()) {});
        }

        public T genericInstanceT() {
            return ReflectUtil.newInstance(getGenericTypeClassT());
        }

        public V genericInstanceV() {
            return ReflectUtil.newInstance(getGenericTypeClassV());
        }
    }

    public static class A { int a; }
    public static class B { int b; }
    public static class AA extends A { int aa; }

    public static class G<T> { T g; }
    public static class GG<T extends G<?>> { T gg; }

    public static void main(String[] args) {
        // TODO: Note the anonymous class syntax {}... it's required to provide a resolution context to TypeToken
        IKnowMyType<A, B> x = new IKnowMyType<>() {};
        A a = x.genericInstanceT();
        B b = x.genericInstanceV();

        IKnowMyType<A, AA> x2 = new IKnowMyType<>() {};
        A a2 = x2.genericInstanceT();
        AA aa = x2.genericInstanceV();

        IKnowMyType<A, B> x3 = new IKnowMyType<>();
        try {
            A a3 = x3.genericInstanceT();
            B b3 = x3.genericInstanceV();
        }
        catch (Exception e) {
            System.out.println("Fails because it does not use anonymous class in:\nIKnowMyType<A, B> x3 = new IKnowMyType<>()");
        }

        IKnowMyType<G<String>, GG<G<Integer>>> x4 = new IKnowMyType<>() {};
        G<String> g1 = x4.genericInstanceT();
        GG<G<Integer>> gg1 = x4.genericInstanceV();

        g1.g = "";
        gg1.gg = new G<>();
        gg1.gg.g = 1;

        String str = new IKnowMyType<String, A>() {}.genericInstanceT();
        System.out.println(str.isBlank());
    }

}
