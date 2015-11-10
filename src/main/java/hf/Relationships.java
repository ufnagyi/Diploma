package hf;

import org.neo4j.graphdb.RelationshipType;

public enum Relationships implements RelationshipType {
    SEEN("buy"), HAS_META("tag"), DIR_BY("dir"), ACTS_IN("act");
    private String property;

    Relationships(String str) {
        this.property = str;
    }

    public String getPropertyName(){
        return property;
    }
}
