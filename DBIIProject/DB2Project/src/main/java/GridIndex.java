import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

public class GridIndex implements Serializable{
    Vector<Dimension> Dimensions;
    Vector<String> colNames ;
    HashMap<String , String> colDataTypes ;
    Vector bucketIDs;
    String id;
    Vector<Vector> pathOfBuckets; //id of the bucket , List of paths of bucket and its overflow

    public GridIndex (){
        this.Dimensions = new Vector<>();
        this.colNames = new Vector<>();
        this.colDataTypes = new HashMap();
        this.pathOfBuckets = new Vector<>();
        this.bucketIDs = new Vector();
    }
    public String toString() {
        return Dimensions.toString();
    }

    public  void ser(String filePath) {
        try {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(this);

            out.close();
            file.close();

            System.out.println("Object has been serialized");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }

    public void deser(String filePath){
        GridIndex GD = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);

            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            GD = (GridIndex) in.readObject();

            in.close();
            file.close();
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
    }
}
