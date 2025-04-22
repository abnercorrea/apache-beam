package org.abnercorreajr.autovalue;

public class AutoValueExampleTest {
    public static void main(String[] args) {
        AutoValueExample ave1 = AutoValueExample.builder().setA(0).setB(1L).build();
        AutoValueExample ave2 = AutoValueExample.builder().setA(10).setB(11L).build();
        AutoValueExample ave3 = AutoValueExample.builder().setA(0).setB(1L).build();

        System.out.printf("%s - hash: %d%n", ave1, ave1.hashCode());
        System.out.printf("%s - hash: %d%n", ave2, ave2.hashCode());
        System.out.printf("%s - hash: %d%n", ave3, ave3.hashCode());

        System.out.printf("ave1 = ave2: %s%n", ave1.equals(ave2));
        System.out.printf("ave1 = ave3: %s%n", ave1.equals(ave3));

        System.out.printf("ave1 changed: %s%n", ave1.toBuilder().setA(100).build());
    }
}
