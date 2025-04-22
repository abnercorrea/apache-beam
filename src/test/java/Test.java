import com.google.common.reflect.TypeToken;
import org.abnercorreajr.util.ReflectUtil;


public class Test {

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
}
