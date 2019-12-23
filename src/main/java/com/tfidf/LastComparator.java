package com.tfidf;

import org.apache.hadoop.io.Text;

public class LastComparator extends Text.Comparator {

    @Override
    public int compare(Object a, Object b) {
        System.out.println("-----------------------------");
        Text aText = (Text) a;
        Text bText = (Text) a;
        String[] splitA = aText.toString().split(":");
        String[] splitB = bText.toString().split(":");
        return Double.compare(Double.parseDouble(splitA[1]),Double.parseDouble(splitB[1]));

    }
}
