package org.isobit.util;

import java.io.Serializable;

public class Tag implements Comparable,Serializable{

    private Object id;
    Object data;
    short flag=0;

    static public boolean compareN(Tag a, Tag b) {
        return (a==null&&b==null)||((a!=null&&b!=null)&&(a.getId().equals(b.getId())));
    }

    public short getFlag() {
        return flag;
    }

    public void setFlag(short flag) {
        this.flag = flag;
    }
 
    public Tag(Object id, Object data) {
        
        this.id = id;
        this.data = data;
    }
    @Override
    public String toString(){
        if(data==null)
            return "";
        else
            return data.toString();
    }
    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Object getId() {
        return id;
    }

    public void setId(Object id) {
        this.id = id;
    }

    public int compareTo(Object o) {
        return this.toString().compareTo(""+o);
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof Tag){
            return this.getId().equals(((Tag)obj).getId());
        }
        return this.getId().equals(obj);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 43 * hash + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }
    
}