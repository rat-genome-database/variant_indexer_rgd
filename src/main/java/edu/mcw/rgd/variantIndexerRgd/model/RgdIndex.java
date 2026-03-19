package edu.mcw.rgd.variantIndexerRgd.model;

import java.util.List;

public class RgdIndex {
    private static RgdIndex instance;

    private String index;
    private String oldAlias;
    private String newAlias;
    private List<String> indices;

    public static RgdIndex getInstance() {
        return instance;
    }

    public static void setInstance(RgdIndex rgdIndex) {
        instance = rgdIndex;
    }

    public String getOldAlias() {
        return oldAlias;
    }

    public void setOldAlias(String oldAlias) {
        this.oldAlias = oldAlias;
    }

    public String getNewAlias() {
        return newAlias;
    }

    public void setNewAlias(String newAlias) {
        this.newAlias = newAlias;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public List<String> getIndices() {
        return indices;
    }

    public void setIndices(List<String> indices) {
        this.indices = indices;
        if (indices != null && indices.size() >= 2) {
            this.newAlias = indices.get(0);
            this.oldAlias = indices.get(1);
        }
    }
}
