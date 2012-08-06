package org.elasticsearch.river.jdbc;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;


import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * Merge the results of the database. Algo is based on specified fieldnames.
 * Differents sperators are authorized like . and [] to specify list of subfield
  */
public class ComplexMerger implements RowListener{

    private ESLogger logger = ESLoggerFactory.getLogger("ComplexMerger");
    private PropertyRoot root;


    public ComplexMerger(){
        this.root = createRoot();
    }

    public PropertyRoot createRoot(){
        PropertyRoot root = new PropertyRoot();
        return root;
    }

    public PropertyRoot getRoot(){
        return this.root;
    }

    public void reset(){
        this.root = new PropertyRoot();
    }

    public PropertyRoot run(String key,String value)throws Exception{
        PropertyRoot root = new PropertyRoot();
        merge(root, key, value);
        return root;
    }


    @Override
    public void row(String operation, String index, String type, String id, List<String> keys, List<Object> values) throws IOException {
        if(keys == null || values == null || keys.size()!=values.size()){
            throw new RuntimeException("Impossible");
        }

        PropertyRoot root = new PropertyRoot("Root");
        for(int i = 0 ; i < keys.size() ; i++){
            try{
                merge(root,keys.get(i),values.get(i));
            }catch(Exception e){
                logger.error("Merge probleme",e);
            }
        }
    }

    @Override
    public void row(List<String> keys, List<Object> values) throws IOException {
        if(keys == null || values == null || keys.size()!=values.size()){
            throw new RuntimeException("Impossible");
        }
        for(int i = 0 ; i < keys.size() ; i++){
            try{
                merge(root,keys.get(i),values.get(i));
            }catch(Exception e){
                logger.error("Probleme merge",e);
            }
            /* To avoid to have two duplicate elements in list */
            root.deleteDuplicate();
        }
    }



    public void merge(PropertyRoot root,String key,Object value)throws Exception{
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

    /**
     * Node structure of JSON representation
     */
    public abstract class PropertyNode {
        String name;

        public String getName() {
            return name;
        }
        /**
         * If the the node is the root
         * @return
         */
        public boolean isRoot(){return false;}

        public void deleteDuplicate(){}

        /**
         * To set the value of a node
         * @param value
         */
        public abstract void addValue(Object value);


        public abstract XContentBuilder getXBuilder(XContentBuilder builder,boolean writeTitle)throws IOException;
    }

    /**
     * Root representation of a tree. Contains some property
     */
    public class PropertyRoot extends PropertyNode{
        private Map<String,PropertyNode> properties = new HashMap<String,PropertyNode>();
        private String id;
        private String operation;
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

        public void addValue(Object value){
            throw new RuntimeException("Impossible");
        }

        public String getOperation() {
            return operation;
        }

        public void setOperation(String operation) {
            this.operation = operation;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        @Override
        public void deleteDuplicate() {
            for(PropertyNode p : properties.values()){
                p.deleteDuplicate();
            }
        }

        public String toString(){
            StringBuilder builder = new StringBuilder();
            builder.append("").append("{");
            int pos = 0;
            for(String s : properties.keySet()){
                builder.append((pos++>0)?",":"").append("\"").append(s).append("\" : ").append(properties.get(s).toString());
            }
            builder.append("}");
            return builder.toString();
        }

        public XContentBuilder getXBuilder()throws IOException{
            return getXBuilder(null,false);
        }

        public XContentBuilder getXBuilder(XContentBuilder builder,boolean writeTitle)throws IOException{
            if(builder == null){
                builder = jsonBuilder();
            }
            if(writeTitle){
                builder.startObject(this.name);
            }else{
                builder.startObject();
            }
            for(String s : properties.keySet()){
                properties.get(s).getXBuilder(builder,true);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PropertyRoot that = (PropertyRoot) o;
            if(properties.size() == 0 && that.properties.size() == 0){return true;}
            if(properties.size()!= that.properties.size()){return false;}

            for(String key : this.properties.keySet()){
                if(!that.properties.containsKey(key)){return false;}
                if(!this.properties.get(key).equals(that.properties.get(key))){return false;}
            }
            
            return true;
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
         * Verify if last element is duplicate and delete it if yes
         */
        public void deleteDuplicate(){
            if(properties.size()>0){
                // We check the previous root not equals to an another element in the list (avoid duplicate)
                // If last equals to another, delete it
                PropertyRoot previousRoot = properties.getLast();
                for(int i = 0 ; i < properties.size() -1 ; i++){    // evict the last element which is the current
                    if(properties.get(i).equals(previousRoot)){
                        properties.removeLast();
                        break;
                    }
                }
            }
        }

        /**
         * Ajoute la propriete dans le dernier PropertyRoot. Si la propriete existe deja, recree un root et l'ajoute a la fin
         * @param propertyNode
         */
        @Override
        public void putProperty(PropertyNode propertyNode) {
            PropertyRoot root = getLast();
            if(root == null || root.containsNode(propertyNode.getName(),null)){
                deleteDuplicate();  // not mandatory
                root = new PropertyRoot("");
                properties.addLast(root);
            }
            root.putProperty(propertyNode);
        }

        /**
         * Special method which use the complete key to test if the node exist.
         * If a node with this complete key exist, need to create a new Property root and define it as last
         * @param name  Name of node
         * @param completeKey Complete key to search in full index if exist
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

         public String toString(){
            StringBuilder builder = new StringBuilder();
            builder.append("[");
            int pos = 0;
            for(PropertyNode node : properties){
                builder.append((pos++>0)?",":"").append(node.toString());
            }
            builder.append("]");
            return builder.toString();
        }

        public XContentBuilder getXBuilder(XContentBuilder builder,boolean writeTitle)throws IOException{
            if(builder == null){
                builder = jsonBuilder();
            }
            builder.startArray(this.name);
            for(PropertyNode node : properties){
                node.getXBuilder(builder,false);
            }
            builder.endArray();

            return builder;

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PropertyListRoot that = (PropertyListRoot) o;
            if(properties.size() == 0 && that.properties.size() == 0){return true;}
            if(properties.size()!= that.properties.size()){return false;}


            for(int i = 0 ; i < this.properties.size() ; i++){
                if(!this.properties.get(i).equals(that.properties.get(i))){return false;}
            }

            return true;
        }

    }


    /**
     * Leaf representation, contains a single value
     * Can stock many values for a same key. The values a stock in a list
     */
    public class PropertyLeaf extends PropertyNode{
        private List<Object> values = new ArrayList<Object>();

        public PropertyLeaf(String key,Object value){
            this.name = key;
            values.add(value);
        }

        /**
         * Set the value of the leaf. If a value have already been set, the new is add in the list (no replace)
         * @param value
         */
        public void addValue(Object value){
            if(!values.contains(value)){
                values.add(value);
            }
        }

        /**
         * Return the differents values of the leaf
         * @return
         */
        public List<Object> getValues() {
            return values;
        }

        public String toString(){
            StringBuilder builder = new StringBuilder();
            int pos = 0;
            for(Object str : values){
                builder.append((pos++ > 0) ? "," : "").append("\"").append(str.toString()).append("\"");
            }
            if(values.size() > 1){
                return "[" + builder.toString() + "]";
            }
            return builder.toString();
        }

        public XContentBuilder getXBuilder(XContentBuilder builder,boolean writeTitle)throws IOException{
            builder.field(this.name);
            if(values.size()>1){
                builder.startArray();
            }
            for(Object str : values){
                builder.value(str);
            }
            if(values.size()>1){
                builder.endArray();
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PropertyLeaf that = (PropertyLeaf) o;

            if(that.getValues().size() == 0 && this.values.size() == 0){return true;}
            if(that.getValues().size()!=this.values.size()){return false;}

            for(int i = 0 ; i < this.values.size() ; i++){
                if(!this.values.get(i).equals(that.getValues().get(i))){return false;}
            }
            return true;
        }
    }
}
