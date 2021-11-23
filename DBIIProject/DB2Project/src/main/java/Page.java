import java.io.*;
import java.util.ArrayList;
import java.util.Vector;

public class Page implements Serializable {
    Vector<Object> rows;
    int id;

    public Page(int id) {
        this.id = id;
        this.rows = new Vector();
    }

    public String toString() {
        return rows.toString();
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

            System.out.println("Object has been serialized");

        } catch (IOException ex) {
            System.out.println("IOException is caught");
        }
    }
    public void deser(String filePath){
        Page pagetoInsertIn = null;
        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filePath);

            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            pagetoInsertIn = (Page) in.readObject();

            in.close();
            file.close();
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
    }
}
