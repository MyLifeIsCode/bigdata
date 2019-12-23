package com.bjsxtpr;


import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Arrays;

public class Node {

    private String key;
    private double pageRank;
    private String[] adjacentNodeNames;

    public static final char fieldSeparator = '\t';

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public double getPageRank() {
        return pageRank;
    }

    public void setPageRank(double pageRank) {
        this.pageRank = pageRank;
    }

    public String[] getAdjacentNodeNames() {
        return adjacentNodeNames;
    }

    public void setAdjacentNodeNames(String[] adjacentNodeNames) {
        this.adjacentNodeNames = adjacentNodeNames;
    }

    public boolean constainsAdjances(){
        return adjacentNodeNames != null && adjacentNodeNames.length > 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(pageRank);

        if (getAdjacentNodeNames() != null) {
            sb.append(fieldSeparator).append(
                    StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
        }
        return sb.toString();
    }

    // value =1.0 B D
    public static Node formatNode(String value) throws IOException {
        String[] parts = StringUtils.splitPreserveAllTokens(value,
                fieldSeparator);
//        System.out.println("----------------------" + value);
        if (parts.length < 1) {
            throw new IOException("Expected 1 or more parts but received "
                    + parts.length);
        }
        Node node = new Node();
        node.setPageRank(Double.valueOf(parts[0]));
        if (parts.length > 1) {
            node.setAdjacentNodeNames(Arrays
                    .copyOfRange(parts, 1, parts.length));
        }
        return node;
    }
    public static Node formatNode(String v1,String v2) throws IOException {
        System.out.println("----------------------" + v1+fieldSeparator+v2);
        return formatNode(v1+fieldSeparator+v2);
        //1.0	B D
    }
}



















