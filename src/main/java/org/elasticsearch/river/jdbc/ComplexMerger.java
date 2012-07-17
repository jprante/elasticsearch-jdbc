package org.elasticsearch.river.jdbc;

import java.util.*;


/**
 * Merge the results of the database. Algo is based on specified fieldnames.
 * Differents sperators are authorized like . and [] to specify list of subfield
 * Beware : the _id must at the first level
 */
public class ComplexMerger {


    public PropertyRoot createRoot()throws Exception{
        PropertyRoot root = new PropertyRoot("Root");
        return root;
    }

    public PropertyRoot run(String key,String value)throws Exception{
        PropertyRoot root = new PropertyRoot();
        merge(root,key,value);
        return root;
    }


    public void merge(PropertyRoot root,String key,String value)throws Exception{
        /* Simple case, with value or multi value, leaf */
        if(!key.contains(".") && !key.contains("[")){
            if(root.containsNode(key)){
                PropertyNode node = root.getNode(key);
                if(node.isRoot()){
                    throw new Exception("Suxx");
                }
                node.addValue(value);
            }
            else{
                root.putProperty(new PropertyLeaf(key,value));
            }
            return;
        }
        /* Case with a list of object */
        if(key.indexOf("[") !=-1 && (key.indexOf("[") < key.indexOf(".") || key.indexOf(".") == -1)){
            // Extract the content in []
            String rootKey = key.substring(0,key.indexOf("["));
            String subContent = key.substring(key.indexOf("[")+1,key.indexOf("]"));
            // If not property have been created, we create it
            PropertyListRoot list = null;
            if(root.containsNode(rootKey)){
                PropertyNode node = root.getNode(rootKey);
                if(!node.isRoot()){
                    throw new Exception("Bad configuration");
                }
                list = (PropertyListRoot)node;
            }
            else{
                list = new PropertyListRoot(rootKey);
            }
            root.putProperty(list);
            merge(list,subContent,value);
            return;
        }

        /* Case with sub object and property */
        if(key.contains(".")){
            String rootKey = key.substring(0,key.indexOf("."));
            String subKey = key.substring(key.indexOf(".")+1);
            if(!root.containsNode(rootKey,key)){
                PropertyRoot subRoot = new PropertyRoot(rootKey);
                root.putProperty(subRoot);
            }
            merge((PropertyRoot)root.getNode(rootKey),subKey,value);
            return;
        }

    }


    public interface JSONable{
        String toJSON();
    }


    /**
     * Node structure of JSON representation
     */
    public abstract class PropertyNode implements JSONable{
        String name;

        public String getName() {
            return name;
        }
        /**
         * If the the node is the root
         * @return
         */
        public boolean isRoot(){return false;}

        /**
         * To set the value of a node
         * @param value
         */
        public abstract void addValue(String value);

        public String toString(){
            return toJSON();
        }
    }

    /**
     * Root representation of a tree. Contains some property
     */
    public class PropertyRoot extends PropertyNode{
        private Map<String,PropertyNode> properties = new HashMap<String,PropertyNode>();

        public PropertyRoot(){}

        public PropertyRoot(String name){
            this.name = name;
        }

        /**
         * Put a new property in the root
         * @param propertyNode
         */
        public void putProperty(PropertyNode propertyNode){
            properties.put(propertyNode.getName(),propertyNode);
        }

        public boolean isRoot(){return true;}


        public PropertyNode getNode(String name){
            return properties.get(name);
        }
        public boolean containsNode(String name,String completeKey){
            return containsNode(name);
        }

        public boolean containsNode(String name){
            return properties.containsKey(name);
        }

        public void addValue(String value){
            throw new RuntimeException("Impossible");
        }

        public String toJSON(){
            StringBuilder builder = new StringBuilder();
            builder.append("").append("{");
            int pos = 0;
            for(String s : properties.keySet()){
                builder.append((pos++>0)?",":"").append("\"").append(s).append("\" : ").append(properties.get(s).toJSON());
            }
            builder.append("}");
            return builder.toString();
        }
    }


    /**
     * Can stock many properties.
     * Use to lead [] structure
     * Use an internal map to know when to create a new element
     */
    public class PropertyListRoot extends PropertyRoot{
        private LinkedList<PropertyRoot> properties = new LinkedList<PropertyRoot>();
        /* use to know if a node has already been add to the properties. Based on the complete key */
        private List<String> keyList = new ArrayList<String>();

        public PropertyListRoot(String name){
            this.name = name;
        }

        public boolean isRoot(){return true;}

        /**
         * Renvoie un node qui ne possede pas de valeur
         * S'il n'existe pas ou s'il contient deja le nom, on en cree un nouveau qu'on ajoute a la fin
         * @param name
         * @return
         */
        public PropertyNode getNode(String name){
            PropertyRoot root = getLast();
            if(root == null || root.containsNode(name,null)){
                root = new PropertyRoot("");
                properties.addLast(root);
            }
            return root;
        }

        /**
         * Ajoute la propriete dans le dernier PropertyRoot. Si la propriete existe deja, recree un root et l'ajoute a la fin
         * @param propertyNode
         */
        @Override
        public void putProperty(PropertyNode propertyNode) {
            PropertyRoot root = getLast();
            if(root == null || root.containsNode(propertyNode.getName(),null)){
                root = new PropertyRoot("");
                properties.addLast(root);
            }
            root.putProperty(propertyNode);
        }

        /**
         * Special method which use the complete key to test if the node exist.
         * If a node with this complete key exist, need to create a new Property root and define it as last
         * @param name
         * @param completeKey
         * @return
         */
        public boolean containsNode(String name,String completeKey){

            if(completeKey!=null){
                if(keyList.contains(completeKey)){
                    // Case when we have already visit this node. Create a new Root
                    PropertyRoot root = new PropertyRoot("");
                    properties.addLast(root);
                    keyList.removeAll(keyList);
                }else{
                    keyList.add(completeKey);
                }
                // To avoid creating root by calling method, all is lead here
                return true;
            }
            return false;
        }

        public boolean containsNode(String name){
            return false;
        }

        public void addValue(String value){
            throw new RuntimeException("Impossible");
        }

        private PropertyRoot getLast(){
            if(properties.size() == 0){
                return null;
            }
            return properties.getLast();
        }

         public String toJSON(){
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            int pos = 0;
            for(PropertyNode node : properties){
                builder.append((pos++>0)?",":"").append(node.toJSON());
            }
            builder.append("]");
            return builder.toString();
        }
    }


    /**
     * Leaf representation, contains a single value
     * Can stock many values for a same key. The values a stock in a list
     */
    public class PropertyLeaf extends PropertyNode{
        private List<String> values = new ArrayList<String>();

        public PropertyLeaf(String key,String value){
            this.name = key;
            values.add(value);
        }

        /**
         * Set the value of the leaf. If a value have already been set, the new is add in the list (no replace)
         * @param value
         */
        public void addValue(String value){
            values.add(value);
        }

        /**
         * Return the differents values of the leaf
         * @return
         */
        public List<String> getValues() {
            return values;
        }

        public String toJSON(){
            StringBuilder builder = new StringBuilder();
            int pos = 0;
            for(String str : values){
                builder.append((pos++ > 0) ? "," : "").append("\"").append(str).append("\"");
            }
            if(values.size() > 1){
                return "[" + builder.toString() + "]";
            }
            return builder.toString();
        }
    }
}
