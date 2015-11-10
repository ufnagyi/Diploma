package hf;

import org.neo4j.graphdb.Label;

import java.util.HashSet;

public class NodeLabel implements Label {
    public String name;
    public String idName;
    public HashSet<String> properties;

    public NodeLabel(String name, String idName){
        this.name = name;
        this.idName = idName;
    }

    public String name(){
        return this.name;
    }

    public String getIdName(){
        return this.idName;
    }

    public void addProperty(String prop){
        properties.add(prop);
    }

    public String[] getProperties(){
        return (String[]) properties.toArray();
    }
}
