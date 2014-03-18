package com.codetroopers.play.elasticsearch;

import java.util.Map;

public interface Indexable {

    public Map toIndex();
    public Indexable fromIndex(Map map);
}
