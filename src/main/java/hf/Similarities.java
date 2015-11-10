package hf;

import org.neo4j.graphdb.RelationshipType;


public enum Similarities implements RelationshipType {
    CF_ISIM("sim");
    private String property;

    Similarities(String str) {
        this.property = str;
    }

    public String getPropertyName(){
        return property;
    }
}
