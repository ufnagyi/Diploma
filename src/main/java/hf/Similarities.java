package hf;

import org.neo4j.graphdb.RelationshipType;


public enum Similarities implements RelationshipType {
    CF_ISIM("sim"), CBF_SIM("sim"), CBF_SIM2("sim"), CBF_SIM3("sim");
    private String property;

    Similarities(String str) {
        this.property = str;
    }

    public String getPropertyName(){
        return property;
    }
}
