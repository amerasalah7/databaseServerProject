import java.io.*;
import java.util.Vector;

public class Database implements Serializable {
    String name;
    Vector<Table> tables;

    public Database(String name) {
        this.name = name;
        this.tables = new Vector<Table>();
    }

    public void ser(String path) {
        try {
            //Saving of object in a file
//            String path = "src/main/resources/data/" + dataBaseName + ".ser";
            FileOutputStream file = new FileOutputStream(path);
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

//    public void deser(String filename) {
////        String filename = "src/main/resources/data/" + dataBaseName + ".ser";
//        Database database = null;
//        try {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream(filename);
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            database = (Database) in.readObject();
//
//            in.close();
//            file.close();
//            System.out.println("deserilized1");
//
//        } catch (IOException ex) {
//            System.out.println("IOException is caught");
//        } catch (ClassNotFoundException ex) {
//            System.out.println("ClassNotFoundException is caught");
//        }
//
//    }
public static Database deser(String file) throws IOException, ClassNotFoundException {
    FileInputStream fileInputStream = new FileInputStream(file);
    BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
    ObjectInputStream objectInputStream = new ObjectInputStream(bufferedInputStream);
    Database object =(Database) objectInputStream.readObject();
    objectInputStream.close();
    return object;
}
}
