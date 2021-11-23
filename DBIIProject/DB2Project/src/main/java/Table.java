import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    String table_name;
    String super_key;
    Hashtable attributes;
    Hashtable colMin;
    Hashtable colMax;
    Vector<HashMap> pages ;
    Vector<Object> ids;
    Vector<Vector> indicies ; // first entery path of the GD, 2nd entery list of strings of the col of the index

//    transient ArrayList<Page> pages = new ArrayList();

    public Table(String Table_name, String super_key, Hashtable attributes,Hashtable colMin,Hashtable colMax) {

        this.attributes = attributes;
        this.table_name = Table_name;
        this.super_key = super_key;
        this.colMin = colMin ;
        this.colMax = colMax;
        pages = new Vector<HashMap>() ;
        ids = new Vector<Object>();
        indicies = new Vector<>();

    }

//    public String toString() {
//        String s="";
//        for(int i = 0; i<this.pages.size();i++)
//            s += this.pages.get(i).v.toString();
//        return s;
//    }
}