package hf;

import org.neo4j.graphdb.Label;


public enum Labels implements Label {
    Item("ItemID"), User("UserID"), VOD("word"), Actor("name"), Director("name");
    private String property;

    Labels(String str) {
        this.property = str;
    }

    public String getPropertyName(){
        return property;
    }
}
