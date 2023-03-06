package org.example;

import org.apache.commons.collections.IteratorUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class IteratorDemo {

    @Test
    public void testIteratorNext() {
        Iterator<String> oneElement = Arrays.asList("one", "TWO").iterator();
/*        String one = oneElement.hasNext() ? oneElement.next() : "";
    System.out.println(one);*/
        List list = IteratorUtils.toList(oneElement, 3);
    System.out.println(list);
    }
}
