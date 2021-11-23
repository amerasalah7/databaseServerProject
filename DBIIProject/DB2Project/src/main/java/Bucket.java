import java.io.*;
import java.util.HashMap;
import java.util.Vector;

public class Bucket implements Serializable{
    String ID ;
    boolean overflow ; // Am Ian overflow??
    int idOfBucket ;
    String path ;
    //min and maxmuim
    boolean hasOverflow;  //Do I have overflows
    String idOfOverflow = null;
    Vector<Vector>Items ;
    Vector<String []> pathOfOverflows; // it has the path over the overflows and the corr value of o
    //in each vector i will store the path of the page at index 0 and store the primary key of the of item  at index 1
    public Bucket(String id){
        this.ID = id;
        this.Items = new Vector<>();
        this.pathOfOverflows = new Vector<>();
    }
    public void ser(String filePath) {
        try {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filePath);
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(this);

            out.close();
            file.close();

//            System.out.println("Object has been serialized");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }

    public void deser(String filename) {
        Bucket bk = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            bk = (Bucket) in.readObject();

            in.close();
            file.close();
            System.out.println("deserilized1");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }

    }
    
}
