package hf.GraphUtils;

import org.neo4j.graphdb.Label;


public enum Labels implements Label {
    Item("ItemID", "title"), User("UserID"), VOD("", "word"), Actor("", "name"), Director("", "name");
    private String property;
    private String idName;

    Labels(String idN) {
        this.idName = idN;
        this.property = "";
    }

    Labels(String idN, String str) {
        this.idName = idN;
        this.property = str;
    }

    public String getPropertyName(){
        return property;
    }

    public String getIDName() {
        return idName;
    }

    public String getUniqueName(){return !this.idName.equals("") ? this.idName : this.property;}
}
