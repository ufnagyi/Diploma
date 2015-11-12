package hf;

import org.neo4j.graphdb.Label;


public enum Labels implements Label {
    Item("ItemID", "title"), User("UserID"), VOD("VodID", "word"), Actor("ActorID", "name"), Director("DirID", "name");
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
}
