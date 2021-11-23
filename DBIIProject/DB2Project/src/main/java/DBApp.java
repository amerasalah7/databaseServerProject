import javax.swing.text.DateFormatter;
import javax.xml.crypto.Data;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.*;

import static java.util.Collections.binarySearch;

public class DBApp implements DBAppInterface {
    static FileWriter csvwriter;
    static String dataBaseName;
    static int maxPage = maxRow();
    static int maxIndex = maxIndex();


    public static int maxIndex() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {

        }
        try {
            prop.load(is);
        } catch (IOException ex) {
        }
        return Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
    }

    public void init() {
        File fileData = new File("src/main/resources/data");
        if (!(fileData.exists())) {
            fileData.mkdir();
        }
        Database a = new Database("DB2");
        try {
            File file = new File("src/main/resources/metadata.csv");
            if (!file.exists()) {
                file.createNewFile();
                csvwriter = new FileWriter(file.getAbsoluteFile(), true);
                csvwriter.append("Table Name");
                csvwriter.append(",");
                csvwriter.append("Column Name");
                csvwriter.append(",");
                csvwriter.append("Column Type");
                csvwriter.append(",");
                csvwriter.append("ClusteringKey");
                csvwriter.append(",");
                csvwriter.append("Indexed");
                csvwriter.append(",");
                csvwriter.append("min");
                csvwriter.append(",");
                csvwriter.append("max");
                csvwriter.append("\n");
                System.out.println("here");
            } else {
                csvwriter = new FileWriter(file.getAbsoluteFile(), true);
            }
            dataBaseName = "DB2";
            String filePath = "src/main/resources/data/" + dataBaseName + ".ser";
            System.out.println(filePath);
            if (!((new File(filePath)).exists())) {
                try {
                    //Saving of object in a file
                    FileOutputStream file2 = new FileOutputStream(filePath);
                    ObjectOutputStream out = new ObjectOutputStream(file2);

                    // Method for serialization of object
                    out.writeObject(a);

                    out.close();
                    file2.close();

                    System.out.println("Object has been serialized");

                } catch (IOException ex) {
                    System.out.println("IOException is caught");
                }
            }


        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                csvwriter.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
                            Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {

        //deserilize the database
        // deserializing the datbase to read from it
        String filename = "src/main/resources/data/" + dataBaseName + ".ser";
        Database database = null;


        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            database = (Database) in.readObject();


            System.out.println("Deserialized");

            in.close();
            file.close();
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }


//        ArrayList<String> tables = namOfTables();
        boolean tableExist = false;

        // check if the table is already exist, do not exist the table twice


        for (int i = 0; i < database.tables.size(); i++) {
            System.out.println(database.tables.get(i).table_name);
            if (tableName.equals(database.tables.get(i).table_name)) {
                tableExist = true;
            }
        }


        // if the table do not exist, now create it

        if (tableExist == false) {
            Table t = new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax);

            // get all the col names in a list then loop to get the primary key and check that is exist ,
            // we will use it also , to put all the cols
            // colName is hashTable and keys are the names and the value is the datatype

            Object[] keys = colNameType.keySet().toArray();

            // the loop for the check

            boolean PKExist = false;

            for (int i = 0; i < keys.length; i++) {
                if (clusteringKey.equals(keys[i]))
                    PKExist = true;
            }

            // Now you can create the table

            if (PKExist == true) {
                System.out.println("hii");
                try {

                    for (int i = 0; i < keys.length; i++) {

                        csvwriter.append(tableName);
                        csvwriter.append(",");
                        csvwriter.append((String) keys[i]); // Added the typcast to be able to append(hwa 2aly 23ml kda)
                        csvwriter.append(",");
                        csvwriter.append(colNameType.get((String) keys[i])); // bec it is hashTable, we can get the
                        // value from the key
                        csvwriter.append(",");
                        if (keys[i].equals(clusteringKey))
                            csvwriter.append("True");
                        else
                            csvwriter.append("False");
                        csvwriter.append(",");
                        csvwriter.append("Indexed");
                        csvwriter.append(",");
                        csvwriter.append(colNameMin.get((String) keys[i]));
                        csvwriter.append(",");
                        csvwriter.append(colNameMax.get((String) keys[i]));
                        csvwriter.append("\n");
                        csvwriter.flush();


                    }
                    System.out.println("added");

                    //     csvwriter.close();

//					System.out.println("added");

                } catch (IOException e) {
                    e.printStackTrace();
                }


                database.tables.add(t);
                String filePath = "src/main/resources/data/" + dataBaseName + ".ser";


                try {
                    //Saving of object in a file
                    FileOutputStream file = new FileOutputStream(filePath);
                    ObjectOutputStream out = new ObjectOutputStream(file);


                    // Method for serialization of object
                    out.writeObject(database);

                    out.close();
                    file.close();

                    System.out.println("Object has been serialized");

                } catch (IOException ex) {
                    System.out.println("IOException is caught");
                }


            } else {
                throw new DBAppException();
            }


        } else {
            throw new DBAppException();

        }


    }


    public ArrayList namOfTables() {
        ArrayList<String> tableNames = new ArrayList();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        try {
//         //   br.readLine(); // read one line at a time, this reads the first line that has the tags.
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        try {
            while (br.ready()) { // as long as the buffer(file) is not empty read
                String x = null;
                try {
                    x = br.readLine(); // read the line and save it in x
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(","); // make the split beacuse the line cont things other than the table name
                if (!tableNames.contains(arr[0])) { // check if it already exist in my array
                    tableNames.add(arr[0]); // first word before the comma is the table name
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return tableNames;

    }

    ////////////////////////////////////createIndex//////////////////////////////////////
    @Override
    public void createIndex(String tableName, String[] columnNames) throws DBAppException, IOException, ClassNotFoundException {
        // 1)check if those cols exist in the table and the table itself exist
        ArrayList<String> tables = namOfTables(); // all names of the table
        ArrayList Coloumns = ColoumnsInAtable(tableName); // col in the target table

        boolean tableExist = false;
        for (int i = 0; i < tables.size(); i++) {
            if (tableName.equals(tables.get(i))) {
                tableExist = true;
            }

        }
        if (tableExist == false)
            throw new DBAppException();
        else {
            ////////change in the metadata file the inexed part of the cols to be True////
            for (int i = 0; i < columnNames.length; i++) {
                makeIndexedTrue(tableName, columnNames[i]);
            }

            ///////////////////////////////creating the grid/////////////////////////
            for (int k = 0; k < columnNames.length; k++) {
                System.out.print(columnNames[k] + " , ");
                if (!Coloumns.contains(columnNames[k])) {
                    throw new DBAppException();
                }
            }
            GridIndex Gd = new GridIndex();
            Gd.id = tableName;
            for (int i = 0; i < columnNames.length; i++) {
                String min = minOfCol(tableName, columnNames[i]);
                String max = maxOfCol(tableName, columnNames[i]); // here I need to check the datatypes
                String dataType = dataTypeOfcol(tableName, columnNames[i]);
                Gd.colDataTypes.put(columnNames[i], dataType); // now I have the data type of each col
                ArrayList<String[]> dimRange = new ArrayList<>();

//                    System.out.println(Gd.colDataTypes.get(columnNames[i]));
                if (Gd.colDataTypes.get(columnNames[i]).equals("java.lang.Integer")) {
                    dimRange = ranges(Integer.parseInt(min), Integer.parseInt(max));
                } else if (Gd.colDataTypes.get(columnNames[i]).equals("java.util.Date")) {
//                        System.out.println("I am a date");
                    dimRange = rangesDate(parseDate(min), parseDate(max));
                } else if (Gd.colDataTypes.get(columnNames[i]).equals("java.lang.String")) {
                    dimRange = rangesString(min, max);
                } else { // double
                    dimRange = rangesDouble(Double.parseDouble(min), Double.parseDouble(max));
                }
                Dimension d = new Dimension(dimRange);
                // Add the ranges of this dimension also the name f the corresponding dim
                Gd.Dimensions.add(d);//[ranges l col]
                Gd.colNames.add(columnNames[i]);//colNames col

                // append the colNames to the id of the GD tp form the id alonge with the table name
                Gd.id += columnNames[i];
            }
            //should I do anything here like serialize aw 7aga??

            // Now deserialize DB to check to get the table pages and insert into the buckets
//                String filename = "src/main/resources/data/" + dataBaseName + ".ser";
//                Database database = null;
            ////////////
            String filename = "src/main/resources/data/" + dataBaseName + ".ser";
            Database database = null;


            try {
                // Reading the object from a file
                FileInputStream file = new FileInputStream(filename);
                ObjectInputStream in = new ObjectInputStream(file);

                // Method for deserialization of object
                database = (Database) in.readObject();


                System.out.println("Deserialized");

                in.close();
                file.close();
            } catch (IOException ex) {
                System.out.println("IOException is caught");
            } catch (ClassNotFoundException ex) {
                System.out.println("ClassNotFoundException is caught");
            }
            //////////////

//                database.deser(filename);

            // here I added the new GD to the list of the GDs of the table lolololoyyyy
            for (int i = 0; i < database.tables.size(); i++) {
                if (database.tables.get(i).table_name.equals(tableName)) {
                    Vector v = new Vector();
                    v.add("src/main/resources/data/" + Gd.id + ".ser");
                    v.add(Gd.colNames);
                    database.tables.get(i).indicies.add(v);
                    break;
                }
            }
            database.ser(filename);
            System.out.println(database.tables.size());
            for (int i = 0; i < database.tables.size(); i++) {
                System.out.println(database.tables.get(i).table_name + " " + (tableName));
                // found the targeted tale
                if (database.tables.get(i).table_name.equals(tableName)) {
                    // loop over all the pages
//                        System.out.println(database.tables.get(i));
                    for (int j = 0; j < database.tables.get(i).pages.size(); j++) {
                        String path = (String) database.tables.get(i).pages.get(j).get(0);// getting the path of the page
//                            Page curPage = new Page(database.tables.get(i).pages.get(j).id);
//                            curPage.deser(path); //now I deser the page, so I can access the enteries

                        Page curPage = null;
                        try {
                            // Reading the object from a file
                            FileInputStream file = new FileInputStream(path);
                            ObjectInputStream in = new ObjectInputStream(file);

                            // Method for deserialization of object
                            curPage = (Page) in.readObject();


                            System.out.println("Deserialized");

                            in.close();
                            file.close();
                        } catch (IOException ex) {
                            System.out.println("IOException is caught");
                        } catch (ClassNotFoundException ex) {
                            System.out.println("ClassNotFoundException is caught");
                        }

                        // Now loop on the page rows
                        for (int k = 0; k < curPage.rows.size(); k++) {
                            // now I need to compare using the value of the col I am building the index on
                            // so I need to have the indicies of those col(building the index on them) -> method -> getIndixfCol
                            String bucketID = "";
                            for (int m = 0; m < Gd.colNames.size(); m++) { // Also here each entery i am gonna check an get the ranges for , I have to know its datatypes
                                if (Gd.colDataTypes.get(Gd.colNames.get(m)).equals("java.lang.Integer")) { // interger case
                                    int indexOfCol = getIndixfCol(tableName, Gd.colNames.get(m)); // now I have the index that I need to check its value
                                    int valueToCheck = (Integer) (((Vector) curPage.rows.get(k)).get(indexOfCol)); // Now I have the value
                                    int whichDimen = binarySearchOnRangesINT(Gd.Dimensions.get(m).arr, valueToCheck); // Is it correct??? int case
                                    bucketID += whichDimen;  // Append on the bucketId
                                } else if (Gd.colDataTypes.get(Gd.colNames.get(m)).equals("java.util.Date")) { // Date case
                                    int indexOfCol = getIndixfCol(tableName, Gd.colNames.get(m)); // now I have the index that I need to check its value

                                    Date valueToCheck = ((Date) ((Vector) curPage.rows.get(k)).get(indexOfCol)); // Now I have the value
                                    int whichDimen = binarySearchOnRangesDate(Gd.Dimensions.get(m).arr, valueToCheck); // Is it correct???
                                    bucketID += whichDimen;  // Append on the bucketId
                                } else if (Gd.colDataTypes.get(Gd.colNames.get(m)).equals("java.lang.String")) { // Date case
                                    int indexOfCol = getIndixfCol(tableName, Gd.colNames.get(m)); // now I have the index that I need to check its value

                                    String valueToCheck = ((String) ((Vector) curPage.rows.get(k)).get(indexOfCol)); // Now I have the value
                                    int whichDimen = binarySearchOnRangesString(Gd.Dimensions.get(m).arr, valueToCheck); // Is it correct???
                                    bucketID += whichDimen;  // Append on the bucketId
                                } else {  // double
                                    int indexOfCol = getIndixfCol(tableName, Gd.colNames.get(m)); // now I have the index that I need to check its value

                                    Double valueToCheck = ((Double) ((Vector) curPage.rows.get(k)).get(indexOfCol)); // Now I have the value
                                    int whichDimen = binarySearchOnRangesDouble(Gd.Dimensions.get(m).arr, valueToCheck); // Is it correct???
                                    bucketID += whichDimen;  // Append on the bucketId

                                }


                            }
                            // Now check if you already have this bucket or you will create new one
                            String bkID = bucketID;
                            int indexOfPK = getPKIndex(tableName);
                            Object PKOFTHeEntery = ((Vector) curPage.rows.get(k)).get(indexOfPK);
                            if (Gd.bucketIDs.contains(bkID)) { // If already the bucket exist
                                System.out.println(bkID);
                                // I think it is better to have a method for bucket insertion
                                // it takes PK of the entery and the path of the page of that element , path of the bucket -> Void
                                String filePathOfBucket = "src/main/resources/data/" + "bucket" + Gd.id + bkID + ".ser";
                                // desr the bucket to give to the method as it is
                                Bucket curBk = null;

//                                    Bucket bk = null;
                                try {
                                    // Reading the object from a file
                                    FileInputStream file = new FileInputStream(filePathOfBucket);
                                    ObjectInputStream in = new ObjectInputStream(file);

                                    // Method for deserialization of object
                                    curBk = (Bucket) in.readObject();

                                    in.close();
                                    file.close();
                                    System.out.println("deserilized1");

                                } catch (IOException ex) {
                                    System.out.println("IOException is caught");
                                } catch (ClassNotFoundException ex) {
                                    System.out.println("ClassNotFoundException is caught");
                                }


//                                    curBk.deser(filePathOfBucket);
//                                    System.out.println(curBk.ID +"hiiiiiii");
                                insertIntoBucket(curBk, PKOFTHeEntery, path, Gd); // what am I gonna do ??
                                // I think using the id I will get the path of the bucket
                                // then deserialize , check if it has overflows -> get to the last overflow -> if full create new overfloe
                                // if not -> insert in it
                                // if it has no overflows -> check if it full -> create an overflow,
                                // if not create -> insert
                                // then serialize the bucket
                                curBk.ser(filePathOfBucket);
                            } else { // I need to create a new one
                                Bucket bk = new Bucket(bkID); // Now I created a new bucket
                                Vector temp = new Vector();
                                temp.add(((Vector) curPage.rows.get(k)).get(indexOfPK));
                                temp.add(path); // first I put the path of page of this entery
                                bk.idOfBucket = 0 ;
                                //Now I have the value of pk of this entery, so put it
                                bk.Items.add(temp);// Now I added the whole vector to the bucket
                                Gd.bucketIDs.add(bkID); // Add it to the list of bucket ids of this index
                                // now ser using the path that has the id -> bkID
                                String filePath = "src/main/resources/data/" + "bucket" + Gd.id + bkID + ".ser";
                                System.out.println(filePath);
                                bk.path = filePath;
                                Vector bV = new Vector();
                                Vector paths = new Vector();
                                bV.add(bkID);
                                paths.add(filePath);
                                bV.add(paths);
                                Gd.pathOfBuckets.add(bV);
                                bk.ser(filePath);
                            }
                        }
                        // here when I finish the page I need to ser it??
                        curPage.ser(path);

                    }
                    // here I finisher all the pages , byb2o gayeem wara b3d fa h3ml break
                    break;
                }
            }

            // here out of the largest loop, I need to ser the grid, and the database itself

            Gd.ser("src/main/resources/data/" + Gd.id + ".ser"); // is the path 7lw kda?


        }

    }


    ///////////////////////////createIndex helper methods//////////////////////////////////////////////

    public static String minOfCol(String tableName, String colName) {
        BufferedReader br = null;
        String min = "0";
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            while (br.ready()) {
                String x = null;
                try {
                    x = br.readLine();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (tableName.equals(arr[0]) && colName.equals(arr[1])) {
                    min = arr[5];
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return String.valueOf(min);

    }

    public static String maxOfCol(String tableName, String colName) {
        BufferedReader br = null;
        String max = "0";
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            while (br.ready()) {
                String x = null;
                try {
                    x = br.readLine();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (tableName.equals(arr[0]) && colName.equals(arr[1])) {
                    max = arr[6];
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return String.valueOf(max);

    }

    public static void makeIndexedTrue(String tableName, String colName) {
        BufferedReader br;
        boolean flag = false;
        int c = 0;
        try {
            String oldtext = "";
            String oldString = "";
            String nedeed = "";
            String newtext = "";
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while (br.ready()) {
                String x = br.readLine();
                String[] arr = x.split(",");
                if (arr[0].equals(tableName) && arr[1].equals(colName)) {
                    flag = true;
                    oldString = x;
                    for (int i = 0; i < 4; i++) {
                        nedeed += arr[i] + ",";
                    }
                    nedeed += "True" + ",";
                    nedeed += arr[5] + ",";
                    nedeed += arr[6];

                }
                oldtext += x + "\r\n";
            }
            if (flag == true) {
                newtext = oldtext.replaceAll(oldString, nedeed);
                FileWriter writer = new FileWriter("src/main/resources/metadata.csv");
                writer.write(newtext);
                writer.close();
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static String dataTypeOfcol(String tableName, String colName) {
        BufferedReader br = null;
        String dataType = "";
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            while (br.ready()) {
                String x = null;
                try {
                    x = br.readLine();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (tableName.equals(arr[0]) && colName.equals(arr[1])) {
                    dataType = arr[2];
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return dataType;
    }

    public static ArrayList ranges(int min, int max) {
        double x = (double) (max - min) / 10;
        ArrayList<String[]> res = new ArrayList();
        int val = (int) Math.ceil(x);
        for (int i = 0; i < 10; i++) {
            String[] temp = new String[2];
            temp[0] = String.valueOf(min);
            temp[1] = String.valueOf(min + val);
            res.add(temp);
            min = min + val + 1;
        }

        return res;
    }

    public static int binarySearchOnRangesINT(ArrayList<String[]> list, int search) {
        int min = 0;
        int max = list.size() - 1;
        int mid = 0;
        while (min <= max) {
            mid = (min + max) / 2;


            Integer x = Integer.parseInt(list.get(mid)[0]);
            Integer y = Integer.parseInt(list.get(mid)[1]);
            if ((search >= x) && (search <= y)) {   //
                return mid;


            } else if (search > Integer.parseInt(list.get(mid)[1])) {
                //Search after the mid
                min = mid + 1;

            } else if (search < Integer.parseInt(list.get(mid)[0])) {
                //Search before the mid
                max = mid - 1;
            }
//            else
//                return ("Index not found");

        }
        return mid;
    }

    public static int binarySearchOnRangesDate(ArrayList<String[]> list, Date search) {
        int min = 0;
        int max = list.size() - 1;
        int mid = 0;
        while (min <= max) {
            mid = (min + max) / 2;


            if (((search.compareTo(parseDate(list.get(mid)[0]))) >= 0) && ((search.compareTo(parseDate((list.get(mid)[1])))) <= 0)) {   //
                return mid;


            } else if (((search.compareTo(parseDate(list.get(mid)[1])))) > 0) {
                //Search after the mid
                min = mid + 1;

            } else if (((search.compareTo(parseDate((list.get(mid)[0])))) < 0)) {
                //Search before the mid
                max = mid - 1;
            }
//            else
//                return ("Index not found");

        }
        return mid;
    }

    public static int binarySearchOnRangesString(ArrayList<String[]> list, String search) {
        int min = 0;
        int max = list.size() - 1;
        int mid = 0;

        while (min <= max) {
            mid = (min + max) / 2;


            if (((search.compareTo(list.get(mid)[0])) >= 0) && ((search.compareTo((list.get(mid)[1]))) <= 0)) {   //
                return mid;


            } else if (((search.compareTo(list.get(mid)[1]))) > 0) {
                //Search after the mid
                min = mid + 1;

            } else if (((search.compareTo((list.get(mid)[0]))) < 0)) {
                //Search before the mid
                max = mid - 1;
            }
//            else
//                return ("Index not found");

        }

        return mid;
    }

    public static int binarySearchOnRangesDouble(ArrayList<String[]> list, Double search) {
        int min = 0;
        int max = list.size() - 1;
        int mid = 0;
        while (min <= max) {
            mid = (min + max) / 2;


            Double x = Double.parseDouble(list.get(mid)[0]);
            Double y = Double.parseDouble(list.get(mid)[1]);
            if ((search >= x) && (search <= y)) {   //
                return mid;


            } else if (search > Double.parseDouble(list.get(mid)[1])) {
                //Search after the mid
                min = mid + 1;

            } else if (search < Double.parseDouble(list.get(mid)[0])) {
                //Search before the mid
                max = mid - 1;
            }
//            else
//                return ("Index not found");

        }
        return mid;
    }

    public static ArrayList rangesString(String min, String max) {

//example
// a,z
//(a,d)(e,g)
// aaa -> zzz (aaa, dzz)(eaa, gzz)


        char first = min.charAt(0);
        char last = max.charAt(0);
        ArrayList<char[]> res = rangesChar(first, last);

        //count how many characters in minimum string    	aaa
        int countMin = 0;
        for (int i = 0; i < min.length(); i++) {
            if (min.charAt(i) != ' ') // kant String 5ltha min
                countMin++;
        }
        int firstCounter = countMin - 1;


        //count how many characters in maximum string    	zzz
        int countMax = 0;
        for (int i = 0; i < max.length(); i++) {
            if (max.charAt(i) != ' ')
                countMax++;
        }
        int lastCounter = countMax - 1;

        //append the minimum character to the first char in the list

        ArrayList<String[]> Final = new ArrayList<>();
        for (int i = 0; i < res.size(); i++) {
            String[] x = new String[2];
            String temp = res.get(i)[0] + ""; //ith list , first element
            String tempTwo = res.get(i)[1] + "";
            for (int j = 0; j < firstCounter; j++) {
                temp += first;
            }
            for (int j = 0; j < lastCounter; j++) {
                tempTwo += last;
            }
            x[0] = temp;
            x[1] = tempTwo;
            Final.add(x);


        }

        return Final;

    }

    public static int getIndixfCol(String tableName, String colName) {
        BufferedReader br = null;
        int index = 0;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            while (br.ready()) {
                String x = null;
                try {
                    x = br.readLine();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (tableName.equals(arr[0])) {
                    if (colName.equals(arr[1])) {
                        break;
                    } else {
                        index++;
                    }
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return index;
    }

    public static ArrayList rangesChar(char min, char max) {

// from char to ascii
        char charMin = min;
        char charMax = max;
        int asciiMin = charMin;
        int asciiMax = charMax;
        int y;


//        double x= (double) (asciiMax- asciiMin)/10;
        int x = (asciiMax - asciiMin) / 10;
        ArrayList<char[]> res = new ArrayList();
        int val = (int) Math.ceil(x);
        for (int i = 0; i < 10; i++) {
            char[] temp = new char[2];

            temp[0] = charMin;
            //from ascii to char
            y = asciiMin + val;
            char[] chars = Character.toChars(y); // chars = min+val
            temp[1] = chars[0];                     // add in temp (2nd position) chars
            res.add(temp);

//            char[] charMin= Character.toChars(y); //min+=val
            y = y + 1;
            chars = Character.toChars(y);
            charMin = chars[0];
            asciiMin = charMin;
            System.out.println(charMin);

        }
        return res;
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////


    public static ArrayList rangesHelperString(char min, char max) {

        // from char to ascii
        char charMin = min;
        char staticMin = min;
        char charMax = max;
        char staticMax = max;
        int asciiMin = charMin;
        int asciiMax = charMax;
        int y;


//	        double x= (double) (asciiMax- asciiMin)/10;
        int x = (asciiMax - asciiMin) / 10;
        ArrayList<char[]> res = new ArrayList();
        int val = (int) Math.ceil(x);
        for (int i = 0; i < 10; i++) {
            char[] temp = new char[2];

            char ok = charMin;
            ok += staticMin;
            temp[0] = ok;   //append static minimum to the minimum value (aa)

            //System.out.println(charMin);
            //from ascii to char
            y = asciiMin + val;
            char[] chars = Character.toChars(y); // chars = min+val

            char ok2 = chars[0];
            ok2 += staticMax;
            temp[1] = ok2;   //append static maximum to the max value (dz)
            res.add(temp);

//	            char[] charMin= Character.toChars(y); //min+=val
            y = y + 1;
            chars = Character.toChars(y);
            charMin = chars[0];
            asciiMin = charMin;
//            System.out.println(charMin);

        }
        return res;
    }

    public static ArrayList rangesDouble(Double min, Double max) {
        ArrayList<String[]> res = new ArrayList<>();
        Double sub = max - min;
        Double val = sub / 10;
        for (int i = 0; i < 10; i++) {
            String[] temp = new String[2];
            temp[0] = String.valueOf(min);
            temp[1] = String.valueOf(min + val);
            res.add(temp);
            min = min + val + 0.1;
        }

        return res;
    }

    public static void insertIntoBucket(Bucket BK, Object PKofEntery, String PagePath, GridIndex Gd) {
        // I have the bucket , so I can check everything lololoyyyyy
        // check if it has overflows
        //vector with path and pk,
        //check if full or not, then check if it has overflow -> if not
        // id of the overflow same but add o + number , number = inc 3la 2lly 2bly
        boolean foundOverflowNotFull = false;
        String[] PathOfLastOverFlowAndVal = null;
        if (BK.Items.size() == maxIndex) {
            // the bucket is full, so you need to check its overflows
            // check first if it already have overflows
            if (BK.hasOverflow) {// if it have -> loop on them until you find one that have a space , if not found -> create new one
                for (int i = 0; i < BK.pathOfOverflows.size(); i++) {
                    Bucket curOverFlow = null;
                    String[] curOverFlowPath = BK.pathOfOverflows.get(i);
                    // now deser it to check if it full or not
//                    curOverFlow.deser(curOverFlowPath[0]);
                    try {
                        // Reading the object from a file
                        FileInputStream file = new FileInputStream(curOverFlowPath[0]);
                        ObjectInputStream in = new ObjectInputStream(file);

                        // Method for deserialization of object
                        curOverFlow = (Bucket) in.readObject();

                        in.close();
                        file.close();
                        System.out.println("deserilized1");

                    } catch (IOException ex) {
                        System.out.println("IOException is caught");
                    } catch (ClassNotFoundException ex) {
                        System.out.println("ClassNotFoundException is caught");
                    }
                    if (curOverFlow.Items.size() < maxIndex) { // if it is not full -> go create new one , if not just loop
                        foundOverflowNotFull = true;
                        //insert in it and break
                        Vector temp = new Vector();
                        temp.add(PKofEntery);
                        temp.add(PagePath);
                        curOverFlow.Items.add(temp);
                        // Now just ser it and go out
                        curOverFlow.deser(curOverFlowPath[0]);
                        break;
                    }
                    // if it is not the case save for me kda kda the path of the curOverflow
                    PathOfLastOverFlowAndVal = curOverFlowPath;// ana htl3 wna ma3aya 2l path da

                }
                if (!foundOverflowNotFull) { // in this case this bool will remain false as its initialization
                    // here I will create a new one
                    int valOfO = Integer.parseInt(PathOfLastOverFlowAndVal[1]) + 1; // parse this val and 1 to it
                    Bucket newOverFlowBucket = new Bucket(BK.ID); // has the same id , but diff path
                    newOverFlowBucket.path = "src/main/resources/data/" + "bucket" + Gd.id + BK.ID + "o" + valOfO + ".ser";
                    for (int p = 0; p < Gd.pathOfBuckets.size(); p++) {
                        if (Gd.pathOfBuckets.get(p).get(0).equals(BK.ID)) {
                            ((Vector) Gd.pathOfBuckets.get(p).get(1)).add("src/main/resources/data/" + "bucket" + Gd.id + BK.ID + "o" + valOfO + ".ser");
                            break;
                        }
                    }
                    newOverFlowBucket.idOfBucket = valOfO ;
                    newOverFlowBucket.overflow = true;
                    BK.hasOverflow = true; // m4 ht5sr fa hktbha
                    String[] x = null;
                    x[0] = newOverFlowBucket.path;
                    x[1] = Integer.toString(valOfO); // tis return string that has the val of 0
                    BK.pathOfOverflows.add(x); // Now I added it to the vect of paths of overflows
                    Vector temp = new Vector();
                    temp.add(PKofEntery);
                    temp.add(PagePath);
                    newOverFlowBucket.Items.add(temp);
                    // now just ser it
                    newOverFlowBucket.ser(newOverFlowBucket.path); // bs kda t2reebn??


                }
            } else {  // if it does not have an overflow -> go create a new one
                String OverflowPath = "src/main/resources/data/" + "bucket" + Gd.id + BK.ID + "o1" + ".ser";
                BK.hasOverflow = true;
                String[] x = new String[2];
                x[0] = OverflowPath;
                x[1] = "1";
                for (int p = 0; p < Gd.pathOfBuckets.size(); p++) {
                    if (Gd.pathOfBuckets.get(p).get(0).equals(BK.ID)) {
                        ((Vector) Gd.pathOfBuckets.get(p).get(1)).add("src/main/resources/data/" + "bucket" + Gd.id + BK.ID + "o1" + ".ser");
                        break;
                    }
                }
                BK.pathOfOverflows.add(x);
                Bucket newOverflowBucket = new Bucket(BK.ID); // it has the same id as the original bucket , but the path has an extra o1
                newOverflowBucket.idOfBucket = 1 ;
                newOverflowBucket.overflow = true; // I am an overflow
                newOverflowBucket.idOfOverflow = BK.ID; // also it has the id of the original one
                newOverflowBucket.path = OverflowPath;
                Vector temp = new Vector();
                temp.add(PKofEntery);
                temp.add(PagePath);
                newOverflowBucket.Items.add(temp); // Now I added the content lolololyyyyyyy
                // just serialize the bucket
                newOverflowBucket.ser(OverflowPath); // the overflow is now serilaized


            }

        } else { // just insert
            Vector temp = new Vector();
            temp.add(PKofEntery);
            temp.add(PagePath);
            BK.Items.add(temp);
        }
        // if the loop looped over all the buckets and did not find an empty one so I need to check here and create a new one

        // do not forget to ser the bucket from the caller


    }

    //add Years
    public static ArrayList rangeDateHelperY(Date min, Date max, int x) {

        String pattern = "yyyy-MM-dd";
        DateFormat df = new SimpleDateFormat(pattern);
        String minAsString = df.format(min);
        String maxAsString = df.format(max);

        LocalDate Date1 = LocalDate.parse(minAsString);
        LocalDate Date2 = LocalDate.parse(maxAsString);
//        Date Date2 = myFormat.parse(max);
        ArrayList<String[]> res = new ArrayList();

        while (Date1.compareTo(Date2) <= 0) {
            for (int i = 0; i < 10; i++) {
                String[] temp = new String[2];

                temp[0] = String.valueOf(Date1);
                temp[1] = String.valueOf(Date1.plusYears(x));
                res.add(temp);
                Date1 = Date1.plusYears(x + 1);

            }
        }
        return res;
    }

    //add days
    public static ArrayList rangeDateHelperD(Date min, Date max, int x) {
        String pattern = "yyyy-MM-dd";
        DateFormat df = new SimpleDateFormat(pattern);
        String minAsString = df.format(min);
        String maxAsString = df.format(max);

        LocalDate Date1 = LocalDate.parse(minAsString);
        LocalDate Date2 = LocalDate.parse(maxAsString);
//        Date Date2 = myFormat.parse(max);
        ArrayList<String[]> res = new ArrayList();

        while (Date1.compareTo(Date2) <= 0) {
            for (int i = 0; i < 10; i++) {
                String[] temp = new String[2];

                temp[0] = String.valueOf(Date1);
                temp[1] = String.valueOf(Date1.plusDays(x));
                res.add(temp);
                Date1 = Date1.plusDays(x + 1);

            }
        }
        return res;
    }


    public static ArrayList rangesDate(Date min, Date max) {


        System.out.println("hello");
//        Date Date1 = myFormat.parse(min);
//        Date Date2 = myFormat.parse(max);
        long diff = max.getTime() - min.getTime();
        int diffDays = (int) (diff / (24 * 60 * 60 * 1000));
        long yearDiff = max.getYear() - min.getYear();
        long monthDiff = max.getMonth() - min.getMonth();
        double x;
        int y;
//        int val = (int) Math.ceil(x);
        int val;
        ArrayList res = new ArrayList();

        if (yearDiff >= 1 && yearDiff <= 11) {
            //4-6-2020, 4-6-2021

            x = (double) diffDays / 10;
            val = (int) Math.ceil(x);
            res = rangeDateHelperD(min, max, val); //range by days
        } else if (yearDiff == 0) {
//            if(monthDiff == 0) { //1-2-2020 , 30-2-2020
            x = (double) diffDays / 10;
            val = (int) Math.ceil(x);
            res = rangeDateHelperD(min, max, val); //range by days
//            }
        } else if (yearDiff > 11) {
            x = (double) yearDiff / 10;
            val = (int) Math.ceil(x);
            res = rangeDateHelperY(min, max, val); //range by years
        }
        System.out.println(res.size());
        return res;

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public static ArrayList<String[]> dataType(String tableName) {
        BufferedReader br = null;
        ArrayList<String[]> tableData = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//     try {
//         br.readLine();
//     } catch (IOException e) {
//         // TODO Auto-generated catch block
//         e.printStackTrace();
//     }
        try {
            while (br.ready()) {
                String x = null;
                try {
                    x = br.readLine();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (tableName.equals(arr[0])) {
                    tableData.add(arr);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return tableData;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    //i use it to loop over the hashtable and get the values
    public static Object checkif(Hashtable<String, Object> colNameValue, String value) {
        Enumeration<String> enumeration = colNameValue.keys();
        Boolean found = false;
        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            if (key.equals(value)) {
                return (Object) colNameValue.get(key);
            }
        }
        return null;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    public static void checkFound(Hashtable<String, Object> colNameValue, ArrayList<String[]> a) throws DBAppException {
        Enumeration<String> enumeration = colNameValue.keys();
        Boolean found = false;
        while (enumeration.hasMoreElements()) {
            String key = enumeration.nextElement();
            for (int i = 0; i < a.size(); i++) {
                if ((a.get(i)[1]).equals(key)) {
                    found = true;
                }
            }
            if (found) {
                found = false;
            } else {
                throw new DBAppException();
            }
        }
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //To check if the date is of the format given
    public boolean isValid(String dateFormat, String date) {
        DateFormat sdf = new SimpleDateFormat(dateFormat);
        sdf.setLenient(false);
        try {
            sdf.parse(date);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////
    // to parse the date
    public String parse(String dateFormat, Date date) {
        DateFormat sdf = new SimpleDateFormat(dateFormat);
//        sdf.setLenient(false);
//        Date x ;
//        try {
//            x = sdf.parse(date);
//        } catch (ParseException e) {
//            return null;
//        }
        //   System.out.println(x);
        return sdf.format(date);
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////
    public static Date parseDate(String date) {
        String[] a = date.split("-");
        int year = Integer.parseInt(a[0]) - 1900;
        int month = Integer.parseInt(a[1]) - 1;
        int day = Integer.parseInt(a[2]);
        Date newDate = new Date(year, month, day);
        return newDate;
    }


    //////////////////////////////////////////////////////////
    public static int maxRow() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {

        }
        try {
            prop.load(is);
        } catch (IOException ex) {
        }
        return Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
    }
////////////////////////////////////////////////////////////////////////////////////////

    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        ArrayList<String[]> dataTypes = dataType(tableName);
        Object pk = null;
        String pkType = null;

        /////////////////////////check the ids of the whole table

//getting the data from csv to check data types and if the primary key is inserted

        if (dataTypes.size() != 0) {
            Vector values = new Vector();
            System.out.println(colNameValue.toString());
            checkFound(colNameValue, dataTypes);


            //starting to check the data types

            for (int j = 0; j < dataTypes.size(); j++) {

                System.out.println(values.toString());
                Object value = checkif(colNameValue, dataTypes.get(j)[1]);

                if (value != null) {
                    System.out.println(value);
                    if (dataTypes.get(j)[2].contains("java.lang.String")) {

                        try {
                            String valueToCompare = (String) value;
                            if (valueToCompare.compareTo(dataTypes.get(j)[5]) >= 0 &&
                                    valueToCompare.compareTo(dataTypes.get(j)[6]) <= 0) {
                                values.add(valueToCompare);

                                if (Boolean.parseBoolean(dataTypes.get(j)[3])) {
                                    pk = value;
                                    pkType = "String";
                                }

                            } else {
                                throw new DBAppException();

                            }
                        } catch (Exception e) {
                            throw new DBAppException();
                        }
                    } else if (dataTypes.get(j)[2].contains("java.lang.Integer")) {

                        try {
                            Integer x = (Integer) value;
                            if (x >= Integer.parseInt(dataTypes.get(j)[5]) && x <= Integer.parseInt(dataTypes.get(j)[6])) {
                                values.add(x);
                                if (Boolean.parseBoolean(dataTypes.get(j)[3])) {
                                    pk = value;
                                    pkType = "Integer";
                                }
                            } else {
                                throw new DBAppException();

                            }
                        } catch (Exception e) {
                            throw new DBAppException();
                        }
                    } else if (dataTypes.get(j)[2].contains("java.lang.Double")) {

                        try {
                            Double x = (Double) value;
                            System.out.println(x);
                            System.out.println(dataTypes.get(j)[5]);
                            System.out.println(dataTypes.get(j)[6]);
                            if (x >= Double.parseDouble(dataTypes.get(j)[5]) && x <= Double.parseDouble(dataTypes.get(j)[6])) {
                                System.out.println("Hereeeeeeeee");
                                values.add(x);
                                if (Boolean.parseBoolean(dataTypes.get(j)[3])) {
                                    pk = value;
                                    pkType = "Double";
                                }
                            } else {
                                System.out.println("Hereeeeeeeee");
                                throw new DBAppException();

                            }
                        } catch (Exception e) {
                            throw new DBAppException();
                        }

                    } else if (dataTypes.get(j)[2].contains("java.util.Date")) {

                        try {
                            Date x = (Date) value;
                            //   System.out.println("WOHOOOOO DATEEEEEEEE");
                            //  String y = x.toString();
                            if (x.compareTo(parseDate(dataTypes.get(j)[5])) >= 0 &&
                                    x.compareTo(parseDate(dataTypes.get(j)[6])) <= 0) {
                                values.add(value);
                                if (Boolean.parseBoolean(dataTypes.get(j)[3])) {
                                    pk = value;
                                    pkType = "Date";
                                }
                            }


                        } catch (Exception e) {
                            throw new DBAppException();
                        }
                    } else {
                        throw new DBAppException();
                    }
                } else {
                    if (Boolean.parseBoolean(dataTypes.get(j)[3])) {
                        throw new DBAppException();
                    } else {
                        values.add(null);
                    }
                }
            }


            //Checked the validity of the values, find the page to insert in it


            // deserializing the datbase to read from it
            String filename = "src/main/resources/data/" + dataBaseName + ".ser";
            Database database = null;
            try {
                // Reading the object from a file
                FileInputStream file = new FileInputStream(filename);
                ObjectInputStream in = new ObjectInputStream(file);

                // Method for deserialization of object
                database = (Database) in.readObject();

                in.close();
                file.close();
                System.out.println("deserilized1");

            } catch (IOException ex) {
                System.out.println("IOException is caught");
            } catch (ClassNotFoundException ex) {
                System.out.println("ClassNotFoundException is caught");
            }


            for (int i = 0; i < database.tables.size(); i++) {


                if (database.tables.get(i).table_name.equals(tableName)) {
                    System.out.println("checking for pages ");

                    /////////////gdeeda
//                     Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (String) pk)
                    if (Collections.binarySearch((Vector) database.tables.get(i).ids, pk) > -1) {
                        throw new DBAppException();
                    }
                    /////////////gdeeda
                    if (database.tables.get(i).pages.size() == 0) {
                        System.out.println("First Page Insertion");
                        Page page = new Page(1);
                        Vector p = new Vector();
                        //    Vector<Object> row = new Vector<Object>();
                        String filePath = "src/main/resources/data/" + tableName + page.id + ".ser";

//                        for (int j = 0; j < values.size(); j++) {
//                            row.add(values.get(j));
//                        }
                        page.rows.add(values);

                        try {
                            //Saving of object in a file
                            FileOutputStream file = new FileOutputStream(filePath);
                            ObjectOutputStream out = new ObjectOutputStream(file);

                            // Method for serialization of object
                            out.writeObject(page);

                            out.close();
                            file.close();

                            System.out.println("Object has been serialized");

                        } catch (IOException ex) {
                            System.out.println("IOException is caught");
                        }

                        HashMap<Integer, Object> page2 = new HashMap<>();
                        page2.put(0, filePath);  //pageFilePath    0
                        page2.put(1, 1);           //id          1
                        page2.put(2, 1);           //numberOfRows     2
                        page2.put(3, pk);         //min  3
                        page2.put(4, pk);        //max   4
                        Vector<Object> ids = new Vector<Object>();   //ids within the page
                        ids.add(pk);
                        page2.put(5, ids);     // ids 5
                        page2.put(6, -1);     //not an overflow 6
                        page2.put(7, false);   //have overflow  7
                        p.add(page2);
                        database.tables.get(i).pages = p;
                        database.tables.get(i).ids.add(pk);

                        System.out.println("Generating the grid indexxxxxxNew Page");
                        ///////////////GRIDIndex////////////////

                        for (int q = 0; q < database.tables.get(i).indicies.size(); q++) {
                            System.out.println("INSERTTTTTTTTT INDEXXXXXXXX New Page");
                            GridIndex grid = null;
                            String filePath9 = (String) ((Vector) database.tables.get(i).indicies.get(q)).get(0);
                            try {
                                // Reading the object from a file
                                FileInputStream file = new FileInputStream(filePath9);

                                ObjectInputStream in = new ObjectInputStream(file);

                                // Method for deserialization of object
                                grid = (GridIndex) in.readObject();

                                in.close();
                                file.close();
                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            } catch (ClassNotFoundException ex) {
                                System.out.println("ClassNotFoundException is caught");
                            }
//                            grid.deser((String)(database.tables.get(i).indicies.get(q)).get(0));
                            Vector columns = (Vector) (database.tables.get(i).indicies.get(q)).get(1);
                            String id = "";
                            System.out.println(id);
                            for (int o = 0; o < columns.size(); o++) {
                                Object x = colNameValue.get(columns.get(o));
                                String dataType = grid.colDataTypes.get(columns.get(o));
                                if (dataType.equals("java.lang.Integer")) {
                                    int z = binarySearchOnRangesINT((grid.Dimensions.get(o)).arr, (Integer) x);
                                    id += z;
                                } else if (dataType.equals("java.lang.Double")) {
                                    int z = binarySearchOnRangesDouble((grid.Dimensions.get(o)).arr, (Double) x);
                                    id += z;
                                } else if (dataType.equals("java.lang.String")) {
                                    int z = binarySearchOnRangesString((grid.Dimensions.get(o)).arr, (String) x);
                                    id += z;

                                } else {
                                    int z = binarySearchOnRangesDate((grid.Dimensions.get(o)).arr, (Date) x);
                                    id += z;

                                }
                            }
                            //since d awl 7aga tt7at f l table f h3ml new bucket 3adii gedan
                            System.out.println(grid.id);
                            System.out.println("the griddd");
                            String path2 = "src/main/resources/data/bucket" + grid.id + id + ".ser";
                            Vector paths = new Vector();
                            paths.add(path2);
                            Vector bucketPath = new Vector();
                            bucketPath.add(id);
                            bucketPath.add(paths);
                            grid.bucketIDs.add(id);
                            System.out.println(grid.bucketIDs.toString());
                            Bucket bucket = new Bucket(id);
                            bucket.idOfBucket = 0 ;
                            bucket.path = path2;
                            bucket.idOfOverflow = null;
                            bucket.hasOverflow = false;
                            bucket.overflow = false;
                            Vector d = new Vector();
                            d.add(pk);
                            d.add(filePath);
                            bucket.Items.add(d);
                            grid.pathOfBuckets.add(bucketPath);
                            //serialize the bucket
                            bucket.ser(path2);
                            grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                            System.out.println((String) (database.tables.get(i).indicies.get(q)).get(0));
                        }


                        //serialize the database Again
                        try {
                            //Saving of object in a file
                            String path = "src/main/resources/data/" + dataBaseName + ".ser";
                            FileOutputStream file = new FileOutputStream(path);
                            ObjectOutputStream out = new ObjectOutputStream(file);

                            // Method for serialization of object
                            out.writeObject(database);

                            out.close();
                            file.close();

                            System.out.println("Object has been serialized");

                        } catch (IOException ex) {
                            System.out.println("IOException is caught");
                        }
                        System.out.println("First page of the index done");
                        break;

                    } else {
                        System.out.println("Second Bucketss is goingg hereee");
                        //  int foundToInsertIn = -1 ;
                        // int inCaseNotFound = -1 ;
                        int indexToBeAdded = -1;
                        Boolean overflow = false;
                        Boolean newPageToBeAdded = false;
                        Boolean noPageAdded = false;
                        System.out.println("Entered here");
                        for (int k = 0; k < database.tables.get(i).pages.size(); k++) {
                            System.out.println("Entered here 2");
                            System.out.println("OVERFLOW OR NOOOOOO" + !(Boolean) database.tables.get(i).pages.get(k).get(7));
                            System.out.println(database.tables.get(i).pages.get(k).toString());
//                            System.out.println("MINNN "+ (String) database.tables.get(i).pages.get(k).get(3));
//                            System.out.println("MAXXXX "+ (String) database.tables.get(i).pages.get(k).get(4));
                            System.out.println(pk);
                            if (pkType.equals("String")) {

                                if (((String) pk).compareTo((String) database.tables.get(i).pages.get(k).get(3)) >= 0 &&
                                        ((String) pk).compareTo((String) database.tables.get(i).pages.get(k).get(4)) <= 0) {

                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;

                                        }
                                        indexToBeAdded = k;
                                        System.out.println("BREAKKKKKKKKKKKK");
                                        break;
                                    }

                                } else if (k > 0 && ((String) pk).compareTo((String) database.tables.get(i).pages.get(k).get(3)) < 0) {
                                    System.out.println("DA5ALLLLLLLLLLLLLL");
                                    System.out.println(((String) pk).compareTo((String) database.tables.get(i).pages.get(k - 1).get(4)));
//                                    if( ((String) pk).compareTo((String) database.tables.get(i).pages.get(k-1).get(4)) >= 0)
//                                    {
//                                        System.out.println("DA5ALLLLLLLLLLLLLL iffff");
//                                        System.out.println(((String) pk).compareTo((String) database.tables.get(i).pages.get(k-1).get(4)) >= 0);
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
//                                    }

                                } else if (k == 0 && ((String) pk).compareTo((String) database.tables.get(i).pages.get(k).get(3)) < 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
                                } else if (k == database.tables.get(i).pages.size() - 1 && ((String) pk).compareTo((String) database.tables.get(i).pages.get(k).get(4)) > 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {

                                        if (((Integer) database.tables.get(i).pages.get(k).get(6)) != -1) {
                                            newPageToBeAdded = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }

                                }

                            } else if (pkType.equals("Integer")) {

                                if (((Integer) pk).compareTo((Integer) database.tables.get(i).pages.get(k).get(3)) >= 0 &&
                                        ((Integer) pk).compareTo((Integer) database.tables.get(i).pages.get(k).get(4)) <= 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;

                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }

                                } else if (k > 0 && ((Integer) pk).compareTo((Integer) database.tables.get(i).pages.get(k).get(3)) < 0) {
//                                    if( ((Integer) pk).compareTo((Integer) database.tables.get(i).pages.get(k-1).get(4)) > 0)
//                                    {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
//                                    }

                                } else if (k == 0 && ((Integer) pk).compareTo((Integer) database.tables.get(i).pages.get(k).get(3)) < 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
                                } else if (k == database.tables.get(i).pages.size() - 1 && ((Integer) pk).compareTo((Integer) database.tables.get(i).pages.get(k).get(4)) > 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {

                                        if (((Integer) database.tables.get(i).pages.get(k).get(6)) != -1) {
                                            newPageToBeAdded = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }

                                }

                            } else if (pkType.equals("Double")) {

                                if (((Double) pk).compareTo((Double) database.tables.get(i).pages.get(k).get(3)) >= 0 &&
                                        ((Double) pk).compareTo((Double) database.tables.get(i).pages.get(k).get(4)) <= 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;

                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }

                                } else if (k > 0 && ((Double) pk).compareTo((Double) database.tables.get(i).pages.get(k).get(3)) < 0) {
//                                    if( ((Double) pk).compareTo((Double) database.tables.get(i).pages.get(k-1).get(4)) > 0)
//                                    {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
//                                    }

                                } else if (k == 0 && ((Double) pk).compareTo((Double) database.tables.get(i).pages.get(k).get(3)) < 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
                                } else if (k == database.tables.get(i).pages.size() - 1 && ((Double) pk).compareTo((Double) database.tables.get(i).pages.get(k).get(4)) > 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {

                                        if (((Integer) database.tables.get(i).pages.get(k).get(6)) != -1) {
                                            newPageToBeAdded = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
                                }

                            } else {
                                if (((Date) pk).compareTo((Date) database.tables.get(i).pages.get(k).get(3)) >= 0 &&
                                        ((Date) pk).compareTo((Date) database.tables.get(i).pages.get(k).get(4)) <= 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;

                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }

                                } else if (k > 0 && ((Date) pk).compareTo((Date) database.tables.get(i).pages.get(k).get(3)) < 0) {
//                                    if( ((Date) pk).compareTo((Date) database.tables.get(i).pages.get(k-1).get(4)) > 0)
//                                    {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
//                                    }

                                } else if (k == 0 && ((Date) pk).compareTo((Date) database.tables.get(i).pages.get(k).get(3)) < 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {
                                        if ((Integer) database.tables.get(i).pages.get(k).get(2) == maxPage) {
                                            overflow = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
                                } else if (k == database.tables.get(i).pages.size() - 1 && ((Date) pk).compareTo((Date) database.tables.get(i).pages.get(k).get(4)) > 0) {
                                    if (!(Boolean) database.tables.get(i).pages.get(k).get(7)) {

                                        if (((Integer) database.tables.get(i).pages.get(k).get(6)) != -1) {
                                            newPageToBeAdded = true;
                                        } else {
                                            noPageAdded = true;
                                        }
                                        indexToBeAdded = k;
                                        break;
                                    }
                                }
                            }
                        }

                        //we have the needed indices for the pages that we may insert in them
                        int newIndex = 0;
                        //  System.out.println("Existing pages"+destBool.size());


                        //  for (int l = 0; l < destID.size(); l++) {
                        //       System.out.println("NUMBER OF ROWS " + database.tables.get(i).pages.get(foundToInsertIn).get(2));
                        //    System.out.println(destBool.get(l));
                        if (noPageAdded) {
                            //do binary search on the ids
                            System.out.println("EZAYYYY " + ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).size());

                            if (pkType == "String") {
                                if (Collections.binarySearch((Vector<String>) database.tables.get(i).pages.get(indexToBeAdded).get(5), (String) pk) > -1) {
                                    throw new DBAppException();
                                } else {
                                    //search for the index to place it
                                    Vector<String> a = (Vector<String>) database.tables.get(i).pages.get(indexToBeAdded).get(5);
                                    newIndex = binarySearchString(a, 0, a.size(), (String) pk);
                                    if (newIndex == -1) {
                                        newIndex++;
                                    }
                                    if (newIndex == a.size()) {
                                        newIndex--;
                                    }
                                    if (a.get(newIndex).compareTo((String) pk) < 0) {
                                        newIndex++;
                                    }
                                }
                            } else if (pkType == "Integer") {
                                if (Collections.binarySearch((Vector<Integer>) database.tables.get(i).pages.get(indexToBeAdded).get(5), (Integer) pk) > -1) {
                                    throw new DBAppException();
                                } else {
                                    //search for the index to place it
                                    Vector<Integer> a = (Vector<Integer>) database.tables.get(i).pages.get(indexToBeAdded).get(5);
                                    newIndex = binarySearchInteger(a, 0, a.size(), (Integer) pk);
                                    if (newIndex == -1) {
                                        newIndex++;
                                    }
                                    if (newIndex == a.size()) {
                                        newIndex--;
                                    }
                                    if (a.get(newIndex).compareTo((Integer) pk) < 0) {
                                        newIndex++;
                                    }
                                }
                            } else if (pkType == "Double") {
                                if (Collections.binarySearch((Vector<Double>) database.tables.get(i).pages.get(indexToBeAdded).get(5), (Double) pk) > -1) {
                                    throw new DBAppException();
                                } else {
                                    //search for the index to place it
                                    Vector<Double> a = (Vector<Double>) database.tables.get(i).pages.get(indexToBeAdded).get(5);
                                    newIndex = binarySearchDouble(a, 0, a.size(), (Double) pk);
                                    if (newIndex == -1) {
                                        newIndex++;
                                    }
                                    if (newIndex == a.size()) {
                                        newIndex--;
                                    }
                                    if (a.get(newIndex).compareTo((Double) pk) < 0) {
                                        newIndex++;
                                    }
                                }
                            } else {
                                if (Collections.binarySearch((Vector<Date>) database.tables.get(i).pages.get(indexToBeAdded).get(5), (Date) pk) > -1) {
                                    throw new DBAppException();
                                } else {
                                    //search for the index to place it
                                    Vector<Date> a = (Vector<Date>) database.tables.get(i).pages.get(indexToBeAdded).get(5);
                                    newIndex = binarySearchDate(a, 0, a.size(), (Date) pk);
                                    if (newIndex == -1) {
                                        newIndex++;
                                    }
                                    if (newIndex == a.size()) {
                                        newIndex--;
                                    }
                                    if (a.get(newIndex).compareTo((Date) pk) < 0) {
                                        newIndex++;
                                    }
                                }
                            }
                            //deserializing the page
                            Page pagetoInsertIn = null;
//                                String o = (String)database.tables.get(i).pages.get(destID.get(l)).get(0) ;
//                                System.out.println(o);
                            try {
                                // Reading the object from a file
                                FileInputStream file = new FileInputStream((String) database.tables.get(i).pages.get(indexToBeAdded).get(0));

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


                            //inserting the row in its correct position
                            //     Vector<Object> newRows = new Vector<Object>();
//                                for (int m = 0; m < pagetoInsertIn.rows.size(); m++) {
//                                    if (newIndex == m) {
//                                        newRows.add(values);
//                                    }
//                                    newRows.add(pagetoInsertIn.rows.get(m));
//                                    if (newIndex == pagetoInsertIn.rows.size()) {
//                                        if(m == newIndex - 1) {
//                                            newRows.add(values);
//                                        }
//                                    }
//                                }
                            if (newIndex == pagetoInsertIn.rows.size()) {
                                pagetoInsertIn.rows.add(values);

                            } else {
                                System.out.println("GOTTT HEREEEE ");
                                Object a = pagetoInsertIn.rows.get(newIndex);
                                Object toBe = pagetoInsertIn.rows.get(newIndex);
                                System.out.println("GOTTT HEREEEE " + a);
                                pagetoInsertIn.rows.set(newIndex, values);
                                //   System.out.println(((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).toString());
                                int sizeToStop = pagetoInsertIn.rows.size();
                                for (int q = newIndex; q < sizeToStop; q++) {
                                    a = toBe;
                                    if (q != sizeToStop - 1) {

                                        toBe = pagetoInsertIn.rows.get(q + 1);
                                        pagetoInsertIn.rows.set(q + 1, a);
                                    } else {
                                        System.out.println("I AM HEREEEEEEEEEEEEEEEE");
                                        pagetoInsertIn.rows.add(a);
                                    }


                                }
                            }


                            //  pagetoInsertIn.rows = newRows;
                            //   database.tables.get(i).pages.get(newIndex).get(2)

                            System.out.println(database.tables.get(i).pages.get(indexToBeAdded).get(5).toString());
                            //updating the tables in the database
                            //      Vector newIDS = new Vector();

//                                for (int q = 0; q < ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).size(); q++) {
//                                    if (newIndex == q) {
//                                        newIDS.add(pk);
//                                    }
//
//                                    newIDS.add(((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).get(q));
//
//                                    if (newIndex == ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).size()) {
//                                        if (q == newIndex - 1) {
//                                            newIDS.add(pk);
//                                        }
//                                    }
//                                }
                            if (newIndex == ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).size()) {
                                ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).add(pk);

                            } else {
                                System.out.println("GOTTT HEREEEE ");
                                Object a = ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).get(newIndex);
                                Object toBe = ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).get(newIndex);
                                System.out.println("GOTTT HEREEEE " + a);
                                ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).set(newIndex, pk);
                                System.out.println(((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).toString());
                                int sizeToStop = ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).size();
                                for (int q = newIndex; q < sizeToStop; q++) {
                                    a = toBe;
                                    if (q != sizeToStop - 1) {

                                        toBe = ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).get(q + 1);
                                        ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).set(q + 1, a);
                                    } else {
                                        System.out.println("I AM HEREEEEEEEEEEEEEEEE");
                                        ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).add(a);
                                    }


                                }
                            }


                            //  database.tables.get(i).pages.get(indexToBeAdded).put(5, newIDS);
                            Integer e = ((Integer) database.tables.get(i).pages.get(indexToBeAdded).get(2));
                            e += 1;
                            database.tables.get(i).pages.get(indexToBeAdded).put(2, e);
                            database.tables.get(i).ids.add(pk);
                            if (((Integer) database.tables.get(i).pages.get(indexToBeAdded).get(6)) == -1) { //ana orginal
                                database.tables.get(i).pages.get(indexToBeAdded).put(3, ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).firstElement());
                                database.tables.get(i).pages.get(indexToBeAdded).put(4, ((Vector) database.tables.get(i).pages.get(indexToBeAdded).get(5)).lastElement());

                                if ((Boolean) database.tables.get(i).pages.get(indexToBeAdded).get(7)) {
                                    int counter = i;
                                    while ((Boolean) database.tables.get(counter).pages.get(indexToBeAdded).get(7)) {
                                        database.tables.get(counter + 1).pages.get(indexToBeAdded).put(3, ((Vector) database.tables.get(counter).pages.get(indexToBeAdded).get(5)).firstElement());
                                        database.tables.get(counter + 1).pages.get(indexToBeAdded).put(4, ((Vector) database.tables.get(counter).pages.get(indexToBeAdded).get(5)).lastElement());
                                        counter++;
                                    }

                                }

                            }


                            //seriliaze the page again

                            try {
                                //Saving of object in a file
                                FileOutputStream file = new FileOutputStream((String) database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                ObjectOutputStream out = new ObjectOutputStream(file);

                                // Method for serialization of object
                                out.writeObject(pagetoInsertIn);

                                out.close();
                                file.close();

                                System.out.println("Object has been serialized");

                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            }

                            //finished from inserting in the page
                            //inserting into the bucket gridindex

//System.out.println("INSERTTTTTTTTT INDEXXXXXXXX");
                            for (int q = 0; q < database.tables.get(i).indicies.size(); q++) {
                                System.out.println("INSERTTTTTTTTT INDEXXXXXXXX NO NEW PAGE");
                                GridIndex grid = null;
                                String filePath9 = (String) ((Vector) database.tables.get(i).indicies.get(q)).get(0);
                                try {
                                    // Reading the object from a file
                                    FileInputStream file = new FileInputStream(filePath9);

                                    ObjectInputStream in = new ObjectInputStream(file);

                                    // Method for deserialization of object
                                    grid = (GridIndex) in.readObject();

                                    in.close();
                                    file.close();
                                } catch (IOException ex) {
                                    System.out.println("IOException is caught");
                                } catch (ClassNotFoundException ex) {
                                    System.out.println("ClassNotFoundException is caught");
                                }
                                System.out.println(grid.id);
                                System.out.println("index related");
                                System.out.println(grid.bucketIDs.toString());
                                System.out.println(grid.pathOfBuckets.toString());
                                Vector columns = (Vector) (database.tables.get(i).indicies.get(q)).get(1);
                                String id = "";
                                for (int o = 0; o < columns.size(); o++) {
                                    Object x = colNameValue.get(columns.get(o));
                                    String dataType = grid.colDataTypes.get(columns.get(o));
                                    if (dataType.equals("java.lang.Integer")) {
                                        int z = binarySearchOnRangesINT((grid.Dimensions.get(o)).arr, (Integer) x);
                                        id += z;
                                    } else if (dataType.equals("java.lang.Double")) {
                                        int z = binarySearchOnRangesDouble((grid.Dimensions.get(o)).arr, (Double) x);
                                        id += z;
                                    } else if (dataType.equals("java.lang.String")) {
                                        int z = binarySearchOnRangesString((grid.Dimensions.get(o)).arr, (String) x);
                                        id += z;

                                    } else {
                                        int z = binarySearchOnRangesDate((grid.Dimensions.get(o)).arr, (Date) x);
                                        id += z;

                                    }
                                }
                                System.out.println(id);
                                System.out.println("max index " + maxIndex);
                                ///

                                //since d awl 7aga tt7at f l table f h3ml new bucket 3adii gedan
                                if (grid.bucketIDs.contains(id)) {
                                    System.out.println("the bucket exist elahmdullah");
                                    for (int w = 0; w < grid.pathOfBuckets.size(); w++) {
                                        if (grid.pathOfBuckets.get(w).get(0).equals(id)) {
                                            //found the paths
                                            System.out.println("Found the bucket");
                                            for (int u = 0; u < ((Vector) grid.pathOfBuckets.get(w).get(1)).size(); u++) {
                                                //paths with us
                                                String path2 = (String) ((Vector) grid.pathOfBuckets.get(w).get(1)).get(u);
                                                System.out.println(path2);
                                                Bucket bucketTo = null;
                                                try {
                                                    // Reading the object from a file
                                                    FileInputStream file = new FileInputStream(path2);

                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                    // Method for deserialization of object
                                                    bucketTo = (Bucket) in.readObject();

                                                    in.close();
                                                    file.close();
                                                } catch (IOException ex) {
                                                    System.out.println("IOException is caught");
                                                } catch (ClassNotFoundException ex) {
                                                    System.out.println("ClassNotFoundException is caught");
                                                }
                                                if (bucketTo.Items.size() == maxIndex) {
                                                    if (!bucketTo.hasOverflow) {
                                                        bucketTo.hasOverflow = true;
                                                        Bucket newBucket = new Bucket(id);
                                                        newBucket.hasOverflow = false;
                                                        newBucket.overflow = true;
                                                        newBucket.idOfOverflow = id;
                                                        newBucket.idOfBucket = bucketTo.idOfBucket+1 ;
//                                                        String id0 = null;
//                                                        for (int y = 0; y < bucketTo.pathOfOverflows.size(); y++) {
//                                                            if (((bucketTo.pathOfOverflows.get(y))[0]).equals(path2)) {
//                                                                id0 = bucketTo.pathOfOverflows.get(y)[1];
//                                                                break;
//                                                            }
//                                                        }
                                                        String path4 = "src/main/resources/data/bucket" + grid.id + id + "o" +newBucket.idOfBucket+ ".ser";
                                                        newBucket.path = path4;
                                                        ((Vector) grid.pathOfBuckets.get(w).get(1)).add(path4);
                                                        Vector d = new Vector();
                                                        d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                                        d.add(pk);
                                                        newBucket.Items.add(d);
                                                        bucketTo.ser(path2);
                                                        newBucket.ser(path4);
                                                        grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                                        break;
                                                    }


                                                } else {
                                                    System.out.println("DONT SAYYYYYYYYY");
                                                    Vector d = new Vector();
                                                    d.add(pk);
                                                    d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                                    bucketTo.Items.add(d);
                                                    bucketTo.ser(path2);
                                                    grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    System.out.println("new Bucket with new id");
                                    //new page of new id
                                    Bucket newBucket = new Bucket(id);
                                    grid.bucketIDs.add(id);
                                    newBucket.hasOverflow = false;
                                    newBucket.overflow = false;
                                    newBucket.idOfOverflow = null;
                                    String path4 = "src/main/resources/data/bucket" + grid.id + id + ".ser";
                                    Vector bucketPath = new Vector();
                                    bucketPath.add(id);
                                    Vector paths = new Vector();
                                    paths.add(path4);
                                    bucketPath.add(paths);
                                    newBucket.idOfBucket = 0 ;
                                    grid.pathOfBuckets.add(bucketPath);
                                    System.out.println(bucketPath.toString());
                                    newBucket.path = path4;
                                    Vector d = new Vector();
                                    d.add(pk);
                                    d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                    newBucket.Items.add(d);
                                    newBucket.ser(path4);
                                    grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                    System.out.println("End insert bucket NEW");
                                }
                            }

                            try {
                                //Saving of object in a file
                                String path = "src/main/resources/data/" + dataBaseName + ".ser";
                                FileOutputStream file = new FileOutputStream(path);
                                ObjectOutputStream out = new ObjectOutputStream(file);

                                // Method for serialization of object
                                out.writeObject(database);

                                out.close();
                                file.close();

                                System.out.println("Object has been serialized");

                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            }

                            break;

                        } else if (overflow) {
                            System.out.println(pk);
                            System.out.println("dest id " + indexToBeAdded + 1);
                            System.out.println("INSERTING NEW PAGEEEEEEEEEEEEEE   1");
                            //create page
                            Page newPage = new Page(indexToBeAdded + 1);
                            newPage.rows.add(values);
                            int p = database.tables.get(i).pages.size() - 1;

                            database.tables.get(i).ids.add(pk);
                            //serilize the page
                            try {
                                //Saving of object in a file

                                String path = "src/main/resources/data/" + tableName + ((Integer) database.tables.get(i).pages.get(p).get(1) + 1) + ".ser";
                                FileOutputStream file = new FileOutputStream(path);
                                ObjectOutputStream out = new ObjectOutputStream(file);

                                // Method for serialization of object
                                out.writeObject(newPage);

                                out.close();
                                file.close();

                                System.out.println("Object has been serialized");

                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            }

                            HashMap newPageInTable = new HashMap();
                            newPageInTable.put(0, "src/main/resources/data/" + tableName + ((Integer) database.tables.get(i).pages.get(p).get(1) + 1) + ".ser");
                            newPageInTable.put(1, indexToBeAdded + 1 + 1);
                            newPageInTable.put(2, 1);
                            newPageInTable.put(3, database.tables.get(i).pages.get(indexToBeAdded).get(3));
                            newPageInTable.put(4, database.tables.get(i).pages.get(indexToBeAdded).get(4));
                            Vector<Object> ids = new Vector<Object>();   //ids within the page
                            ids.add(pk);
                            newPageInTable.put(5, ids);
                            newPageInTable.put(6, indexToBeAdded);     //overflow to which page
                            newPageInTable.put(7, false);              // dont have an overflow
                            database.tables.get(i).pages.get(indexToBeAdded).put(7, true);

                            System.out.println("TILL HEREEE");

                            Vector<HashMap> newPages = new Vector<HashMap>();
                            Boolean changeNow = false;
                            for (int n = 0; n < database.tables.get(i).pages.size(); n++) {
                                if (changeNow) {
                                    Integer y = (Integer) database.tables.get(i).pages.get(n).get(1);
                                    y++;
                                    database.tables.get(i).pages.get(n).put(1, y);
                                }
                                System.out.println(database.tables.get(i).pages.get(n).toString());
                                newPages.add(database.tables.get(i).pages.get(n));
                                if (n == indexToBeAdded) {
                                    newPages.add(newPageInTable);
                                    System.out.println("ADDEDDDDDDDDDDDDDDD");
                                    System.out.println(newPageInTable.toString());
                                    changeNow = true;
                                }


                            }


                            database.tables.get(i).pages = newPages;
                            System.out.println(database.tables.get(i).pages.toString());

                            //inserting into the bucket gridindex

                            for (int q = 0; q < database.tables.get(i).indicies.size(); q++) {
                                System.out.println("INSERTTTTTTTTT INDEXXXXXXXX NO NEW PAGE");
                                GridIndex grid = null;
                                String filePath9 = (String) ((Vector) database.tables.get(i).indicies.get(q)).get(0);
                                try {
                                    // Reading the object from a file
                                    FileInputStream file = new FileInputStream(filePath9);

                                    ObjectInputStream in = new ObjectInputStream(file);

                                    // Method for deserialization of object
                                    grid = (GridIndex) in.readObject();

                                    in.close();
                                    file.close();
                                } catch (IOException ex) {
                                    System.out.println("IOException is caught");
                                } catch (ClassNotFoundException ex) {
                                    System.out.println("ClassNotFoundException is caught");
                                }
                                System.out.println("index related");
                                System.out.println(grid.bucketIDs.toString());
                                System.out.println(grid.pathOfBuckets.toString());
                                Vector columns = (Vector) (database.tables.get(i).indicies.get(q)).get(1);
                                String id = "";
                                for (int o = 0; o < columns.size(); o++) {
                                    Object x = colNameValue.get(columns.get(o));
                                    String dataType = grid.colDataTypes.get(columns.get(o));
                                    if (dataType.equals("java.lang.Integer")) {
                                        int z = binarySearchOnRangesINT((grid.Dimensions.get(o)).arr, (Integer) x);
                                        id += z;
                                    } else if (dataType.equals("java.lang.Double")) {
                                        int z = binarySearchOnRangesDouble((grid.Dimensions.get(o)).arr, (Double) x);
                                        id += z;
                                    } else if (dataType.equals("java.lang.String")) {
                                        int z = binarySearchOnRangesString((grid.Dimensions.get(o)).arr, (String) x);
                                        id += z;

                                    } else {
                                        int z = binarySearchOnRangesDate((grid.Dimensions.get(o)).arr, (Date) x);
                                        id += z;

                                    }
                                }
                                System.out.println(id);
                                System.out.println("max index " + maxIndex);
                                ///

                                //since d awl 7aga tt7at f l table f h3ml new bucket 3adii gedan
                                if (grid.bucketIDs.contains(id)) {
                                    System.out.println("the bucket exist elahmdullah");
                                    for (int w = 0; w < grid.pathOfBuckets.size(); w++) {
                                        if (grid.pathOfBuckets.get(w).get(0).equals(id)) {
                                            //found the paths
                                            System.out.println("Found the bucket");
                                            for (int u = 0; u < ((Vector) grid.pathOfBuckets.get(w).get(1)).size(); u++) {
                                                //paths with us
                                                String path2 = (String) ((Vector) grid.pathOfBuckets.get(w).get(1)).get(u);
                                                System.out.println(path2);
                                                Bucket bucketTo = null;
                                                try {
                                                    // Reading the object from a file
                                                    FileInputStream file = new FileInputStream(path2);

                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                    // Method for deserialization of object
                                                    bucketTo = (Bucket) in.readObject();

                                                    in.close();
                                                    file.close();
                                                } catch (IOException ex) {
                                                    System.out.println("IOException is caught");
                                                } catch (ClassNotFoundException ex) {
                                                    System.out.println("ClassNotFoundException is caught");
                                                }
                                                if (bucketTo.Items.size() == maxIndex) {
                                                    if (!bucketTo.hasOverflow) {
                                                        bucketTo.hasOverflow = true;
                                                        Bucket newBucket = new Bucket(id);
                                                        newBucket.hasOverflow = false;
                                                        newBucket.overflow = true;
                                                        newBucket.idOfOverflow = id;
                                                        newBucket.idOfBucket = bucketTo.idOfBucket + 1;
//                                                        String id0 = null;
//                                                        for (int y = 0; y < bucketTo.pathOfOverflows.size(); y++) {
//                                                            if (((bucketTo.pathOfOverflows.get(y))[0]).equals(path2)) {
//                                                                id0 = bucketTo.pathOfOverflows.get(y)[1];
//                                                                break;
//                                                            }
//                                                        }
                                                        String path4 = "src/main/resources/data/bucket" + grid.id + id + "o" + newBucket.idOfBucket + ".ser";
                                                        newBucket.path = path4;
                                                        ((Vector) grid.pathOfBuckets.get(w).get(1)).add(path4);
                                                        Vector d = new Vector();
                                                        d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                                        d.add(pk);
                                                        newBucket.Items.add(d);
                                                        bucketTo.ser(path2);
                                                        newBucket.ser(path4);
                                                        grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                                        break;
                                                    }


                                                } else {
                                                    System.out.println("DONT SAYYYYYYYYY");
                                                    Vector d = new Vector();
                                                    d.add(pk);
                                                    d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                                    bucketTo.Items.add(d);
                                                    bucketTo.ser(path2);
                                                    grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    System.out.println("new Bucket with new id");
                                    //new page of new id
                                    Bucket newBucket = new Bucket(id);
                                    grid.bucketIDs.add(id);
                                    newBucket.hasOverflow = false;
                                    newBucket.overflow = false;
                                    newBucket.idOfOverflow = null;
                                    newBucket.idOfBucket = 0 ;
                                    String path4 = "src/main/resources/data/bucket" + grid.id + id + ".ser";
                                    Vector bucketPath = new Vector();
                                    bucketPath.add(id);
                                    Vector paths = new Vector();
                                    paths.add(path4);
                                    bucketPath.add(paths);
                                    grid.pathOfBuckets.add(bucketPath);
                                    System.out.println(bucketPath.toString());
                                    newBucket.path = path4;
                                    Vector d = new Vector();
                                    d.add(pk);
                                    d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                    newBucket.Items.add(d);
                                    newBucket.ser(path4);
                                    grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                }
                            }

                            //serilize the database
                            try {
                                //Saving of object in a file
                                String path = "src/main/resources/data/" + dataBaseName + ".ser";
                                FileOutputStream file = new FileOutputStream(path);
                                ObjectOutputStream out = new ObjectOutputStream(file);

                                // Method for serialization of object
                                out.writeObject(database);

                                out.close();
                                file.close();

                                System.out.println("Object has been serialized");

                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            }

                            System.out.println("NEW PAGE SUCCESSSSSS");


                            break;


                        } else if (newPageToBeAdded) {
                            // add an overflow page after this page
                            System.out.println(pk);
                            System.out.println("dest id " + (indexToBeAdded + 1 + 1));
                            System.out.println("INSERTING NEW PAGEEEEEEEEEEEEEE   2");
                            //        System.out.println(((Vector) database.tables.get(i).pages.get(indexToBeAdded+1).get(5)).size());
                            Page newPage = new Page(indexToBeAdded + 1 + 1);
                            newPage.rows.add(values);

                            HashMap newPageInTable = new HashMap();
                            int p = database.tables.get(i).pages.size() - 1;

                            newPageInTable.put(0, "src/main/resources/data/" + tableName + ((Integer) database.tables.get(i).pages.get(p).get(1) + 1) + ".ser");
                            newPageInTable.put(1, indexToBeAdded + 1 + 1);
                            newPageInTable.put(2, 1);
                            newPageInTable.put(3, pk);
                            newPageInTable.put(4, pk);
                            Vector<Object> ids = new Vector<Object>();   //ids within the page
                            ids.add(pk);
                            newPageInTable.put(5, ids);
                            newPageInTable.put(6, -1);
                            newPageInTable.put(7, false);
                            database.tables.get(i).ids.add(pk);

//                                    Vector<Vector> newPages = new Vector<Vector>();
////                                    for (int n = 0; n < database.tables.get(i).pages.size(); n++)
////                                        newPages.add(database.tables.get(i).pages.get(n));
////
////                                        if (n  == inCaseNotFound) {
////                                            newPages.add(newPageInTable);
////                                        }
////
//                                        newPages.add(newPageInTable) ;

                            database.tables.get(i).pages.add(newPageInTable);


                            //serilize the page
                            try {
                                //Saving of object in a file
                                String path = "src/main/resources/data/" + tableName + ((Integer) database.tables.get(i).pages.get(p).get(1) + 1) + ".ser";
                                FileOutputStream file = new FileOutputStream(path);
                                ObjectOutputStream out = new ObjectOutputStream(file);

                                // Method for serialization of object
                                out.writeObject(newPage);

                                out.close();
                                file.close();

                                System.out.println("Object has been serialized");

                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            }

                            //inserting into the bucket gridindex

                            for (int q = 0; q < database.tables.get(i).indicies.size(); q++) {
                                System.out.println("INSERTTTTTTTTT INDEXXXXXXXX NO NEW PAGE");
                                GridIndex grid = null;
                                String filePath9 = (String) ((Vector) database.tables.get(i).indicies.get(q)).get(0);
                                try {
                                    // Reading the object from a file
                                    FileInputStream file = new FileInputStream(filePath9);

                                    ObjectInputStream in = new ObjectInputStream(file);

                                    // Method for deserialization of object
                                    grid = (GridIndex) in.readObject();

                                    in.close();
                                    file.close();
                                } catch (IOException ex) {
                                    System.out.println("IOException is caught");
                                } catch (ClassNotFoundException ex) {
                                    System.out.println("ClassNotFoundException is caught");
                                }
                                System.out.println("index related");
                                System.out.println(grid.bucketIDs.toString());
                                System.out.println(grid.pathOfBuckets.toString());
                                Vector columns = (Vector) (database.tables.get(i).indicies.get(q)).get(1);
                                String id = "";
                                for (int o = 0; o < columns.size(); o++) {
                                    Object x = colNameValue.get(columns.get(o));
                                    String dataType = grid.colDataTypes.get(columns.get(o));
                                    if (dataType.equals("java.lang.Integer")) {
                                        int z = binarySearchOnRangesINT((grid.Dimensions.get(o)).arr, (Integer) x);
                                        id += z;
                                    } else if (dataType.equals("java.lang.Double")) {
                                        int z = binarySearchOnRangesDouble((grid.Dimensions.get(o)).arr, (Double) x);
                                        id += z;
                                    } else if (dataType.equals("java.lang.String")) {
                                        int z = binarySearchOnRangesString((grid.Dimensions.get(o)).arr, (String) x);
                                        id += z;

                                    } else {
                                        int z = binarySearchOnRangesDate((grid.Dimensions.get(o)).arr, (Date) x);
                                        id += z;

                                    }
                                }
                                System.out.println(id);
                                System.out.println("max index " + maxIndex);
                                ///

                                //since d awl 7aga tt7at f l table f h3ml new bucket 3adii gedan
                                if (grid.bucketIDs.contains(id)) {
                                    System.out.println("the bucket exist elahmdullah");
                                    for (int w = 0; w < grid.pathOfBuckets.size(); w++) {
                                        if (grid.pathOfBuckets.get(w).get(0).equals(id)) {
                                            //found the paths
                                            System.out.println("Found the bucket");
                                            for (int u = 0; u < ((Vector) grid.pathOfBuckets.get(w).get(1)).size(); u++) {
                                                //paths with us
                                                String path2 = (String) ((Vector) grid.pathOfBuckets.get(w).get(1)).get(u);
                                                System.out.println(path2);
                                                Bucket bucketTo = null;
                                                try {
                                                    // Reading the object from a file
                                                    FileInputStream file = new FileInputStream(path2);

                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                    // Method for deserialization of object
                                                    bucketTo = (Bucket) in.readObject();

                                                    in.close();
                                                    file.close();
                                                } catch (IOException ex) {
                                                    System.out.println("IOException is caught");
                                                } catch (ClassNotFoundException ex) {
                                                    System.out.println("ClassNotFoundException is caught");
                                                }
                                                if (bucketTo.Items.size() == maxIndex) {
                                                    if (!bucketTo.hasOverflow) {
                                                        bucketTo.hasOverflow = true;
                                                        Bucket newBucket = new Bucket(id);
                                                        newBucket.hasOverflow = false;
                                                        newBucket.overflow = true;
                                                        newBucket.idOfOverflow = id;
                                                        newBucket.idOfBucket = bucketTo.idOfBucket + 1;
//                                                        String id0 = null;
//                                                        for (int y = 0; y < bucketTo.pathOfOverflows.size(); y++) {
//                                                            if (((bucketTo.pathOfOverflows.get(y))[0]).equals(path2)) {
//                                                                id0 = bucketTo.pathOfOverflows.get(y)[1];
//                                                                break;
//                                                            }
//                                                        }
                                                        String path4 = "src/main/resources/data/bucket" + grid.id + id + "o" + newBucket.idOfBucket + ".ser";
                                                        newBucket.path = path4;
                                                        ((Vector) grid.pathOfBuckets.get(w).get(1)).add(path4);
                                                        Vector d = new Vector();
                                                        d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                                        d.add(pk);
                                                        newBucket.Items.add(d);
                                                        bucketTo.ser(path2);
                                                        newBucket.ser(path4);
                                                        grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                                        break;
                                                    }


                                                } else {
                                                    System.out.println("DONT SAYYYYYYYYY");
                                                    Vector d = new Vector();
                                                    d.add(pk);
                                                    d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                                    bucketTo.Items.add(d);
                                                    bucketTo.ser(path2);
                                                    grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    System.out.println("new Bucket with new id");
                                    //new page of new id
                                    Bucket newBucket = new Bucket(id);
                                    grid.bucketIDs.add(id);
                                    newBucket.hasOverflow = false;
                                    newBucket.overflow = false;
                                    newBucket.idOfOverflow = null;
                                    newBucket.idOfBucket = 0 ;
                                    String path4 = "src/main/resources/data/bucket" + grid.id + id + ".ser";
                                    Vector bucketPath = new Vector();
                                    bucketPath.add(id);
                                    Vector paths = new Vector();
                                    paths.add(path4);
                                    bucketPath.add(paths);
                                    grid.pathOfBuckets.add(bucketPath);
                                    System.out.println(bucketPath.toString());
                                    newBucket.path = path4;
                                    Vector d = new Vector();
                                    d.add(pk);
                                    d.add(database.tables.get(i).pages.get(indexToBeAdded).get(0));
                                    newBucket.Items.add(d);
                                    newBucket.ser(path4);
                                    grid.ser((String) (database.tables.get(i).indicies.get(q)).get(0));
                                }
                            }
                            //serilize the database

                            try {
                                //Saving of object in a file
                                String path = "src/main/resources/data/" + dataBaseName + ".ser";
                                FileOutputStream file = new FileOutputStream(path);
                                ObjectOutputStream out = new ObjectOutputStream(file);

                                // Method for serialization of object
                                out.writeObject(database);

                                out.close();
                                file.close();

                                System.out.println("Object has been serialized");

                            } catch (IOException ex) {
                                System.out.println("IOException is caught");
                            }
                            break;


                        }

                    }

                } else if (!database.tables.get(i).table_name.equals(tableName) && i == database.tables.size() - 1) {
                    System.out.println("table doesnt exist");
                    throw new DBAppException();
                }


            }
            //serilize the database

            try {
                //Saving of object in a file
                String path = "src/main/resources/data/" + dataBaseName + ".ser";
                FileOutputStream file = new FileOutputStream(path);
                ObjectOutputStream out = new ObjectOutputStream(file);

                // Method for serialization of object
                out.writeObject(database);

                out.close();
                file.close();

                System.out.println("Object has been serialized");

            } catch (IOException ex) {
                System.out.println("IOException is caught");
            }


        } else {
            throw new DBAppException();
        }


    }

    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
            throws DBAppException {
        Table t1 = FindTable(tableName);
        if (t1.indicies.isEmpty()) {
            //normal update
            ArrayList<String> tables = namOfTables();

            BufferedReader br = null;
            boolean tableExist = false;
            ArrayList Coloumns = ColoumnsInAtable(tableName);
            String pk = PrimaryKey(tableName);
            Object[] allUpdatedColoumns = columnNameValue.keySet().toArray();
            for (int i = 0; i < tables.size(); i++) {
                if (tableName.equals(tables.get(i))) {
                    tableExist = true;


                }

            }


            if (tableExist == false)
                throw new DBAppException();
            else {
                Object[] keys = columnNameValue.keySet().toArray();

                System.out.println(Coloumns);
                for (int k = 0; k < keys.length; k++) {
                    System.out.print(keys[k] + " , ");
                    if (!Coloumns.contains(keys[k])) {
                        throw new DBAppException();
                    }
                }

            }

            for (int q = 0; q < allUpdatedColoumns.length; q++) {
                String currentKey = (String) allUpdatedColoumns[q];
                //System.out.println("Current key in all updated coloumns arrAY "+ currentKey);
                String valueDataType = columnNameValue.get(allUpdatedColoumns[q]).getClass().getSimpleName();
                int indexofTheCol = getIndexKey(tableName, currentKey);
                ArrayList dataTypes1 = DatatypesInTable(tableName);

                String rightDatatype = (String) (dataTypes1.get(indexofTheCol));

                if (!(rightDatatype.toLowerCase()).contains(valueDataType.toLowerCase())) {
                    throw new DBAppException();


                }
            }
            Page pag = null;
            try {
                Table t = FindTable(tableName);
                int LastPage = t.pages.size();

                int FirstPage = 0;
                int PageIndex = -1;
                int middlePage = 0;
                for (int y = 0; y < LastPage; y++) {
                    HashMap v = t.pages.get(y);
                    String max = v.get(4) + "";
                    String min = v.get(3) + "";
                    int numOfRows = (int) v.get(2);
                    String path = (String) v.get(0);
                    String pk2 = PrimaryKey(tableName);
                    int indexOfPrimaryKey = getIndexKey(tableName, pk2);

                    ArrayList dataTypesIHave = DatatypesInTable(tableName);

                    String PageMaxdatatype = (String) dataTypesIHave.get(indexOfPrimaryKey);
                    //max.getClass().getSimpleName();


                    System.out.println("pageMaxDatatype " + PageMaxdatatype);
                    if (PageMaxdatatype.contains("String")) {


                        if ((max).compareTo((String) (clusteringKeyValue)) >= 0) {   //if the value is between the minmuim and maxmuim

                            PageIndex = y;
                            break;
                        }
                    }
                    if (PageMaxdatatype.contains("int") || PageMaxdatatype.contains("Int")) {

                        System.out.println("da5alt fe el ineteger coondition ");
                        Integer x = Integer.parseInt(clusteringKeyValue);
                        Integer max1 = Integer.parseInt(max);
                        Integer min1 = Integer.parseInt(min);

                        if (x <= max1) {   //if the value is between the minmuim and maxmuim
                            PageIndex = y;
                            System.out.println("ana fe el else if " + middlePage);
                            break;
                        }
                    }
                    if (PageMaxdatatype.contains("float") || PageMaxdatatype.contains("Float") || PageMaxdatatype.contains("Double")) {
                        // System.out.println("da5alt fe el float coondition ");
                        Float x = Float.parseFloat(clusteringKeyValue);
                        Float max1 = Float.parseFloat(max);
                        Float min1 = Float.parseFloat(min);
                        if (x <= max1) {   //if the value is between the minmuim and maxmuim
                            PageIndex = y;
                            break;
                        }
                    }
                    if (PageMaxdatatype.contains("Date") || PageMaxdatatype.contains("date")) {
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");

//                    Date maxDate = sdformat.parse((String)max);
//                    Date minDate = sdformat.parse((String)min);
//                    Date PKDate = sdformat.parse((String)clusteringKeyValue);
                        Date pk1 = parseDate(clusteringKeyValue);
                        Date min1 = (Date) v.get(3);
                        Date max1 = (Date) v.get(4);

                        if ((max1).compareTo((pk1)) >= 0) {   //if the value is between the minmuim and maxmuim
                            PageIndex = y;
                            break;
                        }
                    }


                }

                System.out.println("PageIndex " + PageIndex);
                if (PageIndex == -1) {
                    System.out.println("H THrow an exception");
                    throw new DBAppException();
                }
                //else {
                HashMap v = t.pages.get(PageIndex);
                //System.out.println(v);

                Vector vectorOfPKs = (Vector) v.get(5);
                ArrayList listOfPKs = new ArrayList<>(vectorOfPKs);
//          int rightIndex= binarySearch(listOfPKs ,clusteringKeyValue);
                System.out.println("list of pks  " + listOfPKs);
                System.out.println(clusteringKeyValue);

                String max = v.get(4) + "";
                String min = v.get(3) + "";
                int numOfRows = (int) v.get(2);
                String path = (String) v.get(0);


                FileInputStream fileIn = new FileInputStream(path);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                pag = (Page) in.readObject();
                in.close();
                fileIn.close();
                int n = numOfRows; // el hya el mafrod zay numOfRows bzbtpag.rows.size()
                //int rowIndex =-1;

                int first = 0;
                int last = n;
                //System.out.println("numberOfRows" +last);
                int mid = 0;
                int rightIndex = -1;
                //binary search for the row betwwen the sorted rows after checking each data type

                System.out.println("da5lt el while el taht ");
                mid = first + (last - first) / 2;


//                    System.out.println("first is : "+ first);
//                    System.out.println("last is : "+ last);
//
//                    System.out.println("middle "+ mid);
                Vector row = (Vector) pag.rows.get(mid);
                String PK = PrimaryKey(tableName);
                int index = getIndexKey(tableName, PK);

                Object rowPrimaryKey = row.get(index);
                String PKdatatype = rowPrimaryKey.getClass().getSimpleName();
                // System.out.println(PKdatatype);
                if (PKdatatype.contains("String")) {
                    rightIndex = binarySearch(listOfPKs, clusteringKeyValue);
                    System.out.println("right index at string " + rightIndex);
//                        if(((String)rowPrimaryKey).compareTo((String)(clusteringKeyValue))<0){
//                            first=mid+1;
//
//                        }
//                        else if (((String)rowPrimaryKey).compareTo((String)(clusteringKeyValue))==0){
//                            rowIndex=mid;
//
//                        }
//                        else {
//                            last=mid;
//                        }
                }

                if (PKdatatype.contains("int") || PKdatatype.contains("Int")) {
                    System.out.println("inyteger conditon taht ");
                    Integer x = Integer.parseInt(clusteringKeyValue);
                    // System.out.println("x :"+ x);
                    // System.out.println("primary key value :"+ (int)rowPrimaryKey);
                    rightIndex = binarySearch(listOfPKs, x);
                    System.out.println("right index at int " + rightIndex);
//                        if(((int)rowPrimaryKey)<x){
//
//                            first=mid;
//
//                        }
//                        else if ((int)rowPrimaryKey==x){
//
//                            rightIndex=mid;
//
//                        }
//                        else {
//
//                            last=mid;
//
//                        }
                }
                if (PKdatatype.contains("float") || PKdatatype.contains("Float") || PKdatatype.contains("Double")) {
                    Double x = Double.parseDouble(clusteringKeyValue);
                    rightIndex = binarySearch(listOfPKs, x);
                    System.out.println("right index at float " + rightIndex);
//                        if(((int)rowPrimaryKey)<x){
//                            first=mid+1;
//                        }
//                        else if ((int)rowPrimaryKey==x){
//                            rowIndex=mid;
//
//                        }
//                        else {
//                            last=mid;
//                        }
                }
                if (PKdatatype.contains("date") || PKdatatype.contains("Date")) {
                    //  SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                    Date d1 = (Date) rowPrimaryKey;
                    Date d2 = parseDate(clusteringKeyValue);
                    rightIndex = binarySearch(listOfPKs, d2);
                    System.out.println("right index at date " + rightIndex);
//                        if(d1.compareTo(d2)<0){
//                            first=mid+1;
//                        }
//                        else if (d1.compareTo(d2)==0){
//                            rowIndex=mid;
//
//                        }
//                        else {
//                            last=mid;
//                        }
                }


                System.out.println("right index" + rightIndex);
                if (rightIndex <= -1) {
                    System.out.println("throw an exceptoin at the right index =-*1");
                    throw new DBAppException();
                } else {

                    Vector requiredRow = (Vector) pag.rows.get(rightIndex);
                    Object[] keys = columnNameValue.keySet().toArray();

                    for (int w = 0; w < keys.length; w++) {
                        int temp = getIndexKey(tableName, (String) keys[w]);
                        requiredRow.remove(temp);
                        Object newValue = columnNameValue.get((String) keys[w]);
                        requiredRow.insertElementAt(newValue, temp);
                    }

                    pag.rows.remove(rightIndex);
                    pag.rows.insertElementAt(requiredRow, rightIndex);


                    try {
                        FileOutputStream fileOut =
                                new FileOutputStream(path);
                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                        out.writeObject(pag);
                        out.close();
                        fileOut.close();

                    } catch (IOException i2) {
                        i2.printStackTrace();
                    }


                }
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            } catch (ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }


        } else {
            //update with insert and delete
//            String pkType1 = PrimaryKey(tableName);
            String pktype = PKdataType(tableName);
            ArrayList<String[]> dataTypes = dataType(tableName);
            checkFound(columnNameValue, dataTypes);
//            ArrayList<Object> pkNeeded = pkUsed(columnNameValue, dataTypes);
            Vector rowValue = null;
            for (int i = 0; i < t1.pages.size(); i++) {
//                String pktype = (String) pkNeeded.get(1);
//                Object pk = pkNeeded.get(2);
                if (pktype.equals("java.lang.String")) {
//
                    String min = (String) t1.pages.get(i).get(3);
                    String max = (String) t1.pages.get(i).get(4);
                    if (((String) clusteringKeyValue).compareTo(min) >= 0 && ((String) clusteringKeyValue).compareTo(max) <= 0
                            || ((String) clusteringKeyValue).compareTo((String) t1.pages.get(i).get(3)) <= 0) {

                        int here = Collections.binarySearch((Vector) t1.pages.get(i).get(5), (String) clusteringKeyValue);

                        if (here > -1) {
                            String path = (String) t1.pages.get(i).get(0);
                            Page page = null;
//                                    System.out.println(path);


                            try {
                                // Reading the object from a file
                                FileInputStream file = new FileInputStream(path);
                                ObjectInputStream in = new ObjectInputStream(file);

                                // Method for deserialization of object
                                page = (Page) in.readObject();


                                System.out.println("Deserialized");

                                in.close();
                                file.close();
                            } catch (IOException | ClassNotFoundException ex) {
                                System.out.println("IOException is caught");
                            }
                            rowValue = (Vector) page.rows.get(here);
                        }
                    }
                } else if (pktype.equals("java.lang.Double")) {
//
                    Double min = (Double) t1.pages.get(i).get(3);
                    Double max = (Double) t1.pages.get(i).get(4);
                    if ((Double.parseDouble(clusteringKeyValue)) >= (min) && (Double.parseDouble(clusteringKeyValue)) <= (max)
                            || (Double.parseDouble(clusteringKeyValue)) <= ((Double) t1.pages.get(i).get(3))) {

                        int here = Collections.binarySearch((Vector) t1.pages.get(i).get(5), (Double.parseDouble(clusteringKeyValue)));

                        if (here > -1) {
                            String path = (String) t1.pages.get(i).get(0);
                            Page page = null;
//                                    System.out.println(path);


                            try {
                                // Reading the object from a file
                                FileInputStream file = new FileInputStream(path);
                                ObjectInputStream in = new ObjectInputStream(file);

                                // Method for deserialization of object
                                page = (Page) in.readObject();


                                System.out.println("Deserialized");

                                in.close();
                                file.close();
                            } catch (IOException | ClassNotFoundException ex) {
                                System.out.println("IOException is caught");
                            }
                            rowValue = (Vector) page.rows.get(here);
                        }
                    }
                } else if (pktype.equals("java.util.Date")) {
//
                    Date min = (Date) t1.pages.get(i).get(3);
                    Date max = (Date) t1.pages.get(i).get(4);
                    if ((parseDate(clusteringKeyValue)).compareTo(min) >= 0 && (parseDate(clusteringKeyValue)).compareTo(max) <= 0
                            || (parseDate(clusteringKeyValue)).compareTo((Date) t1.pages.get(i).get(3)) <= 0) {

                        int here = Collections.binarySearch((Vector) t1.pages.get(i).get(5), parseDate(clusteringKeyValue));

                        if (here > -1) {
                            String path = (String) t1.pages.get(i).get(0);
                            Page page = null;
//                                    System.out.println(path);


                            try {
                                // Reading the object from a file
                                FileInputStream file = new FileInputStream(path);
                                ObjectInputStream in = new ObjectInputStream(file);

                                // Method for deserialization of object
                                page = (Page) in.readObject();


                                System.out.println("Deserialized");

                                in.close();
                                file.close();
                            } catch (IOException | ClassNotFoundException ex) {
                                System.out.println("IOException is caught");
                            }
                            rowValue = (Vector) page.rows.get(here);
                        }
                    }
                } else if (pktype.equals("java.lang.Integer")) {
//
                    Integer min = (Integer) t1.pages.get(i).get(3);
                    Integer max = (Integer) t1.pages.get(i).get(4);
                    if ((Integer.parseInt(clusteringKeyValue)) >= (min) && (Integer.parseInt(clusteringKeyValue)) <= (max)
                            || (Integer.parseInt(clusteringKeyValue)) <= ((Integer) t1.pages.get(i).get(3))) {

                        int here = Collections.binarySearch((Vector) t1.pages.get(i).get(5), Integer.parseInt(clusteringKeyValue));

                        if (here > -1) {
                            String path = (String) t1.pages.get(i).get(0);
                            Page page = null;
//                                    System.out.println(path);


                            try {
                                // Reading the object from a file
                                FileInputStream file = new FileInputStream(path);
                                ObjectInputStream in = new ObjectInputStream(file);

                                // Method for deserialization of object
                                page = (Page) in.readObject();


                                System.out.println("Deserialized");

                                in.close();
                                file.close();
                            } catch (IOException | ClassNotFoundException ex) {
                                System.out.println("IOException is caught");
                            }
                            rowValue = (Vector) page.rows.get(here);
                        }
                    }
                }
                HashMap<Integer, Object> needed = theArrNeeded(columnNameValue, dataTypes);//indices is the key and [datatypes,value]
                Set<Integer> a = needed.keySet();//index array
                for (int k : a) {
//                    Object toBeCompared = rowValue.get(k); //get the value of specifies index
                    ArrayList<Object> ourValue = (ArrayList<Object>) needed.get(k);
                    if (((String) ourValue.get(0)).equals("java.lang.String")) {
                        rowValue.set(k, ourValue.get(1));
                    } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                        rowValue.set(k, ourValue.get(1));
                    } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                        rowValue.set(k, ourValue.get(1));
                    } else if (((String) ourValue.get(0)).equals("java.util.Date")) {
                        rowValue.set(k, ourValue.get(1));
                    }
                }
                //now the row is readyy
                //we can delete here
                String pk1 = PrimaryKey(tableName);
                Hashtable<String, Object> c = new Hashtable<>();
                Object d = null;
                if (pktype.equals("java.lang.String")) {
                    d = (String) clusteringKeyValue;
                } else if (pktype.equals("java.lang.Double")) {
                    d = Double.parseDouble(clusteringKeyValue);
                } else if (pktype.equals("java.util.Date")) {
                    d = parseDate(clusteringKeyValue);
                } else if (pktype.equals("java.lang.Integer")) {
//
                    d = Integer.parseInt(clusteringKeyValue);
                }
                c.put(pk1, d);
                this.deleteFromTable(tableName, c);
                Hashtable result = forInsertion(dataTypes, rowValue);
                this.insertIntoTable(tableName, result);
                break;
            }
        }


        // 0 file path --- 1 id ----- 2 numberofRows -----3 min ------4 max


    }

    public static Hashtable<String, Object> forInsertion(ArrayList<String[]> a, Vector row) {
        Hashtable<String, Object> z = new Hashtable();
        for (int i = 0; i < a.size(); i++) {
            String key = a.get(i)[1];
            z.put(key, row.get(i));
        }
        return z;

    }


    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
        String filename = "src/main/resources/data/" + dataBaseName + ".ser";
        Database database = null;


        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            database = (Database) in.readObject();


            System.out.println("Deserialized");

            in.close();
            file.close();
        } catch (IOException | ClassNotFoundException ex) {
            System.out.println("IOException is caught");
        }

        Boolean found = false;
        int indexFound = -1;

        for (int i = 0; i < database.tables.size(); i++) {
            if (tableName.equals(database.tables.get(i).table_name)) {
                indexFound = i;
                found = true;
                break;
            }
        }
        if (found) {
            if (database.tables.get(indexFound).pages.isEmpty()) {
                System.out.println("TABLEE IS EMPTYYYYYYYYYY");
            } else {
                ArrayList<String[]> dataTypes = dataType(tableName);
                checkFound(columnNameValue, dataTypes);
                System.out.println(columnNameValue.toString());
                if (pkUsed(columnNameValue, dataTypes) != null) {
                    //Case of the primary key is used
                    ArrayList<Object> pkNeeded = pkUsed(columnNameValue, dataTypes);
                    for (int i = 0; i < database.tables.get(indexFound).pages.size(); i++) {

//                        ArrayList<Object> pkNeeded = new ArrayList<>();
                        //   Integer index = (Integer) pkNeeded.get(0);
                        String pktype = (String) pkNeeded.get(1);
                        Object pk = pkNeeded.get(2);
                        String used = "";
                        for (int h = 0; h < database.tables.get(indexFound).indicies.size(); h++) {
                            //n4eel ba2a l ids mn l indices
                            System.out.println("elhamdullah f tani tagrobaaaa");
                            String currentGrid = ((String) database.tables.get(indexFound).indicies.get(h).get(0));
                            //  System.out.println(used);
                            System.out.println(currentGrid);
                            if (!currentGrid.equals(used)) {
                                GridIndex currentGridToDelete = null;
                                String path3 = (String) database.tables.get(indexFound).indicies.get(h).get(0);
                                try {
                                    // Reading the object from a file
                                    FileInputStream file = new FileInputStream(path3);
                                    ObjectInputStream in = new ObjectInputStream(file);

                                    // Method for deserialization of object
                                    currentGridToDelete = (GridIndex) in.readObject();


                                    System.out.println("Deserialized");

                                    in.close();
                                    file.close();
                                } catch (IOException | ClassNotFoundException ex) {
                                    System.out.println("IOException is caught");
                                }
                                System.out.println("entering to the indicesssss");
                                for (int n = 0; n < currentGridToDelete.pathOfBuckets.size(); n++) {
                                    System.out.println("entered indicesssssssss ");

                                    for (int c = 0; c < ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).size(); c++) {
                                        Bucket currentBucketToDelete = null;
                                        boolean flag = false;
                                        String path1 = (String) ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).get(c);
                                        System.out.println(path1);
                                        try {
                                            // Reading the object from a file
                                            FileInputStream file = new FileInputStream(path1);
                                            ObjectInputStream in = new ObjectInputStream(file);

                                            // Method for deserialization of object
                                            currentBucketToDelete = (Bucket) in.readObject();


                                            System.out.println("Deserialized");

                                            in.close();
                                            file.close();
                                        } catch (IOException | ClassNotFoundException ex) {
                                            System.out.println("IOException is caught");
                                        }
                                        for (int b = 0; b < currentBucketToDelete.Items.size(); b++) {
                                            System.out.println(currentBucketToDelete.Items.get(b));
                                            System.out.println(pk);
                                            System.out.println((currentBucketToDelete.Items.get(b)).get(0));
                                            if (pktype.equals("java.lang.Double")) {
                                                if (((Double) (currentBucketToDelete.Items.get(b)).get(0)).equals((Double) pk)) {
                                                    //delete here
                                                    flag = true;
                                                    currentBucketToDelete.Items.removeElementAt(b);
                                                    if (currentBucketToDelete.Items.isEmpty()) {
                                                        //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                        try {
                                                            File f = new File(path1);           //file to be delete
                                                            if (f.delete())                      //returns Boolean value
                                                            {
                                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                            } else {
                                                                System.out.println("failed");
                                                            }
                                                        } catch (Exception e1) {
                                                            e1.printStackTrace();
                                                        }
                                                        ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                        if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                            //removed the id
                                                            String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                            currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                            currentGridToDelete.bucketIDs.removeElement(id);
                                                            currentGridToDelete.ser(currentGrid);
                                                            break;
                                                        } else {
                                                            if (c > 0) {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                bucket5.ser(path10);
                                                            } else {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.overflow = false;
                                                                bucket5.idOfOverflow = null;
                                                                bucket5.ser(path10);
                                                            }

                                                        }

                                                        currentGridToDelete.ser(currentGrid);

                                                    } else {
                                                        currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                        b--;
                                                    }
                                                    break;
                                                }
                                            } else if (pktype.equals("java.lang.String")) {
                                                if (((String) (currentBucketToDelete.Items.get(b)).get(0)).equals((String) pk)) {
                                                    //delete here
                                                    flag = true;
                                                    currentBucketToDelete.Items.removeElementAt(b);
                                                    if (currentBucketToDelete.Items.isEmpty()) {
                                                        //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                        try {
                                                            File f = new File(path1);           //file to be delete
                                                            if (f.delete())                      //returns Boolean value
                                                            {
                                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                            } else {
                                                                System.out.println("failed");
                                                            }
                                                        } catch (Exception e1) {
                                                            e1.printStackTrace();
                                                        }
                                                        ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                        if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                            //removed the id
                                                            String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                            currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                            currentGridToDelete.bucketIDs.removeElement(id);
                                                            currentGridToDelete.ser(currentGrid);
                                                            break;
                                                        } else {
                                                            if (c > 0) {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                bucket5.ser(path10);
                                                            } else {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.overflow = false;
                                                                bucket5.idOfOverflow = null;
                                                                bucket5.ser(path10);
                                                            }

                                                        }
                                                        currentGridToDelete.ser(currentGrid);

                                                    } else {
                                                        currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                        b--;
                                                    }
                                                    break;
                                                }
                                            } else if (pktype.equals("java.util.Date")) {
                                                if (((Date) (currentBucketToDelete.Items.get(b)).get(0)).equals((Date) pk)) {
                                                    //delete here
                                                    flag = true;
                                                    currentBucketToDelete.Items.removeElementAt(b);
                                                    if (currentBucketToDelete.Items.isEmpty()) {
                                                        //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                        try {
                                                            File f = new File(path1);           //file to be delete
                                                            if (f.delete())                      //returns Boolean value
                                                            {
                                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                            } else {
                                                                System.out.println("failed");
                                                            }
                                                        } catch (Exception e1) {
                                                            e1.printStackTrace();
                                                        }
                                                        ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                        if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                            //removed the id
                                                            String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                            System.out.println(id);
                                                            System.out.println(currentGrid);
                                                            currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                            currentGridToDelete.bucketIDs.removeElement(id);
                                                            currentGridToDelete.ser(currentGrid);
                                                            break;
                                                        } else {
                                                            if (c > 0) {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                bucket5.ser(path10);
                                                            } else {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.overflow = false;
                                                                bucket5.idOfOverflow = null;
                                                                bucket5.ser(path10);
                                                            }

                                                        }
                                                        currentGridToDelete.ser(currentGrid);

                                                    } else {
                                                        currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                        b--;
                                                    }
                                                    break;
                                                }
                                            } else if (pktype.equals("java.lang.Integer")) {
                                                if (((Integer) (currentBucketToDelete.Items.get(b)).get(0)).equals((Integer) pk)) {
                                                    //delete here
                                                    flag = true;
                                                    currentBucketToDelete.Items.removeElementAt(b);
                                                    if (currentBucketToDelete.Items.isEmpty()) {
                                                        //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                        try {
                                                            File f = new File(path1);           //file to be delete
                                                            if (f.delete())                      //returns Boolean value
                                                            {
                                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                            } else {
                                                                System.out.println("failed");
                                                            }
                                                        } catch (Exception e1) {
                                                            e1.printStackTrace();
                                                        }
                                                        ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                        if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                            //removed the id
                                                            String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                            currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                            currentGridToDelete.bucketIDs.removeElement(id);
                                                            currentGridToDelete.ser(currentGrid);
                                                            break;
                                                        } else {
                                                            if (c > 0) {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                bucket5.ser(path10);
                                                            } else {
                                                                String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                Bucket bucket5 = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path10);
                                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                                    // Method for deserialization of object
                                                                    bucket5 = (Bucket) in.readObject();
                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                } catch (ClassNotFoundException ex) {
                                                                    System.out.println("ClassNotFoundException is caught");
                                                                }
                                                                bucket5.overflow = false;
                                                                bucket5.idOfOverflow = null;
                                                                bucket5.ser(path10);
                                                            }

                                                        }
                                                        currentGridToDelete.ser(currentGrid);

                                                    } else {
                                                        currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                        b--;
                                                    }
                                                    break;
                                                }
                                            }


                                        }
                                        if (flag){
                                            break;
                                        }



                                    }


                                }


                            }


                        }
                        if (pktype.equals("java.lang.String")) {
//
                            String min = (String) database.tables.get(indexFound).pages.get(i).get(3);
                            String max = (String) database.tables.get(indexFound).pages.get(i).get(4);
                            if (((String) pk).compareTo(min) >= 0 && ((String) pk).compareTo(max) <= 0
                                    || ((String) pk).compareTo((String) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {

                                int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (String) pk);

                                if (here > -1) {
                                    ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                    Page page = null;
//                                    System.out.println(path);


                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(path);
                                        ObjectInputStream in = new ObjectInputStream(file);

                                        // Method for deserialization of object
                                        page = (Page) in.readObject();


                                        System.out.println("Deserialized");

                                        in.close();
                                        file.close();
                                    } catch (IOException | ClassNotFoundException ex) {
                                        System.out.println("IOException is caught");
                                    }
                                    page.rows.remove(here);
                                    database.tables.get(indexFound).ids.removeElement(pk);
//                                    ((Vector)database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                    {
                                        //handle delete of pages
                                        if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                        {


                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                //All its overflow pages should have the new min and max values
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                String newmin = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                String newmax = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                int counter = i + 1;
                                                while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                    database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                    database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                    counter++;
                                                }

                                            } else { //case if it is overflow and has overflow !=-1 & true
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                            }
                                        }
                                        //if I have no overflows
                                        else {
                                            //and I am an overflow to other page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                            }
                                        }

                                        try {
                                            File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                            if (f.delete())                      //returns Boolean value
                                            {
                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                            } else {
                                                System.out.println("failed");
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

                                        database.tables.get(indexFound).pages.remove(i);
                                        i--;
                                    } else {//not empty page
                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                            database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                            ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);//remove the key from the list of ids
//                                            System.out.println("hii"+database.tables.get(indexFound).pages.get(i).get(3));
//                                            //update the min and max of the curr page and all its overflows if I am original
//
//                                            int counter2 = i;
//                                            String newmin2 = (String) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            String newmax2 = (String) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }


                                            //do not forget the rest of cases
                                        }
                                        try {
                                            FileOutputStream fileOut =
                                                    new FileOutputStream(path);
                                            ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                            out.writeObject(page);
                                            out.close();
                                            fileOut.close();

                                        } catch (IOException i2) {
                                            i2.printStackTrace();
                                        }
                                    }
                                    //serialize page

                                    //serialize db
                                    //serilize the database

                                    try {
                                        //Saving of object in a file
                                        String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                        FileOutputStream file = new FileOutputStream(pathdb);
                                        ObjectOutputStream out = new ObjectOutputStream(file);

                                        // Method for serialization of object
                                        out.writeObject(database);

                                        out.close();
                                        file.close();

                                        System.out.println("Object has been serialized");

                                    } catch (IOException ex) {
                                        System.out.println("IOException is caught");
                                    }


                                    break;
                                }

                            }

                        } else if (pktype.equals("java.lang.Integer")) {
                            if ((((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                    ((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                    || ((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Integer) pk);
                                if (here > -1) {
                                    ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                    Page page = null;

                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(path);
                                        ObjectInputStream in = new ObjectInputStream(file);

                                        // Method for deserialization of object
                                        page = (Page) in.readObject();


                                        System.out.println("Deserialized");

                                        in.close();
                                        file.close();
                                    } catch (IOException | ClassNotFoundException ex) {
                                        System.out.println("IOException is caught");
                                    }
                                    page.rows.remove(here);
                                    database.tables.get(indexFound).ids.removeElement(pk);
//                                    ((Vector)database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                    {
                                        //handle delete of pages
                                        if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                        {


                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                //All its overflow pages should have the new min and max values
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                int newmin = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                int newmax = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                int counter = i + 1;
                                                while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                    database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                    database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                    counter++;
                                                }

                                            } else { //case if it is overflow and has overflow !=-1 & true
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                            }
                                        }
                                        //if I have no overflows
                                        else {
                                            //and I am an overflow to other page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                            }
                                        }

                                        try {
                                            File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                            if (f.delete())                      //returns Boolean value
                                            {
                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                            } else {
                                                System.out.println("failed");
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

                                        database.tables.get(indexFound).pages.remove(i);
                                        i--;
                                    } else {//not empty page
                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                            database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                            ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);//remove the key from the list of ids
                                            //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            int newmin2 = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            int newmax2 = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                            //do not forget the rest of cases
                                        }
                                        try {
                                            FileOutputStream fileOut =
                                                    new FileOutputStream(path);
                                            ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                            out.writeObject(page);
                                            out.close();
                                            fileOut.close();

                                        } catch (IOException i2) {
                                            i2.printStackTrace();
                                        }
                                    }
                                    //serialize page

                                    //serialize db
                                    //serilize the database

                                    try {
                                        //Saving of object in a file
                                        String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                        FileOutputStream file = new FileOutputStream(pathdb);
                                        ObjectOutputStream out = new ObjectOutputStream(file);

                                        // Method for serialization of object
                                        out.writeObject(database);

                                        out.close();
                                        file.close();

                                        System.out.println("Object has been serialized");

                                    } catch (IOException ex) {
                                        System.out.println("IOException is caught");
                                    }


                                    break;

                                }

                            }
                        } else if (pktype.equals("java.lang.Double")) {
                            if ((((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                    ((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                    || ((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Double) pk);
                                if (here > -1) {
                                    ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                    Page page = null;

                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(path);
                                        ObjectInputStream in = new ObjectInputStream(file);

                                        // Method for deserialization of object
                                        page = (Page) in.readObject();


                                        System.out.println("Deserialized");

                                        in.close();
                                        file.close();
                                    } catch (IOException | ClassNotFoundException ex) {
                                        System.out.println("IOException is caught");
                                    }
                                    page.rows.remove(here);
                                    database.tables.get(indexFound).ids.removeElement(pk);
//                                    ((Vector)database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                    {
                                        //handle delete of pages
                                        if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                        {


                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                //All its overflow pages should have the new min and max values
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                Double newmin = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                Double newmax = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                int counter = i + 1;
                                                while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                    database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                    database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                    counter++;
                                                }

                                            } else { //case if it is overflow and has overflow !=-1 & true
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                            }
                                        }
                                        //if I have no overflows
                                        else {
                                            //and I am an overflow to other page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                            }
                                        }

                                        try {
                                            File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                            if (f.delete())                      //returns Boolean value
                                            {
                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                            } else {
                                                System.out.println("failed");
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

                                        database.tables.get(indexFound).pages.remove(i);
                                        i--;
                                    } else {//not empty page
                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                            database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                            ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);//remove the key from the list of ids
                                            //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Double newmin2 = (Double) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Double newmax2 = (Double) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                            //do not forget the rest of cases
                                        }
                                        //serialize page
                                        try {
                                            FileOutputStream fileOut =
                                                    new FileOutputStream(path);
                                            ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                            out.writeObject(page);
                                            out.close();
                                            fileOut.close();

                                        } catch (IOException i2) {
                                            i2.printStackTrace();
                                        }
                                    }

                                    //serialize db
                                    //serilize the database

                                    try {
                                        //Saving of object in a file
                                        String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                        FileOutputStream file = new FileOutputStream(pathdb);
                                        ObjectOutputStream out = new ObjectOutputStream(file);

                                        // Method for serialization of object
                                        out.writeObject(database);

                                        out.close();
                                        file.close();

                                        System.out.println("Object has been serialized");

                                    } catch (IOException ex) {
                                        System.out.println("IOException is caught");
                                    }


                                    break;

                                }

                            }
                        } else if (pktype.equals("java.util.Date")) {
                            if ((((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                    ((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                    || ((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Date) pk);
                                if (here > -1) {
                                    ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                    Page page = null;

                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(path);
                                        ObjectInputStream in = new ObjectInputStream(file);

                                        // Method for deserialization of object
                                        page = (Page) in.readObject();


                                        System.out.println("Deserialized");

                                        in.close();
                                        file.close();
                                    } catch (IOException | ClassNotFoundException ex) {
                                        System.out.println("IOException is caught");
                                    }
                                    page.rows.remove(here);
                                    database.tables.get(indexFound).ids.removeElement(pk);
//                                    ((Vector)database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                    if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                    {
                                        //handle delete of pages
                                        if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                        {


                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                //All its overflow pages should have the new min and max values
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                Date newmin = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                Date newmax = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                int counter = i + 1;
                                                while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                    database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                    database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                    counter++;
                                                }

                                            } else { //case if it is overflow and has overflow !=-1 & true
                                                database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                            }
                                        }
                                        //if I have no overflows
                                        else {
                                            //and I am an overflow to other page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                            }
                                        }

                                        try {
                                            File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                            if (f.delete())                      //returns Boolean value
                                            {
                                                System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                            } else {
                                                System.out.println("failed");
                                            }
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

                                        database.tables.get(indexFound).pages.remove(i);
                                        i--;
                                    } else {//not empty page
                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                            database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                            ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);//remove the key from the list of ids
                                            //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Date newmin2 = (Date) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Date newmax2 = (Date) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                            //do not forget the rest of cases
                                        }
                                        //serialize page
                                        try {
                                            FileOutputStream fileOut =
                                                    new FileOutputStream(path);
                                            ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                            out.writeObject(page);
                                            out.close();
                                            fileOut.close();

                                        } catch (IOException i2) {
                                            i2.printStackTrace();
                                        }
                                    }

                                    //serialize db
                                    //serilize the database

                                    try {
                                        //Saving of object in a file
                                        String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                        FileOutputStream file = new FileOutputStream(pathdb);
                                        ObjectOutputStream out = new ObjectOutputStream(file);

                                        // Method for serialization of object
                                        out.writeObject(database);

                                        out.close();
                                        file.close();

                                        System.out.println("Object has been serialized");

                                    } catch (IOException ex) {
                                        System.out.println("IOException is caught");
                                    }


                                    break;

                                }

                            }
                        }

                    }


                } else {
                    // primary key is not used
                    System.out.println("NOO PRIMARY KEYYYYYYYy");
                    String PKType = PKdataType(tableName);
                    if (columnNameValue.isEmpty()) {
                        System.out.println("NO VALUESSSSS ENTEREDDDDDD");
                    } else {
                        if (database.tables.get(indexFound).indicies.isEmpty()) {
                            //NO INDICESSSSSSSSSSS

                            HashMap<Integer, Object> needed = theArrNeeded(columnNameValue, dataTypes);//indices is the key and [datatypes,value]
                            for (int i = 0; i < database.tables.get(indexFound).pages.size(); i++) {
                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                Page page = null;

//                       deserilaze the page
                                try {
                                    // Reading the object from a file
                                    FileInputStream file = new FileInputStream(path);
                                    ObjectInputStream in = new ObjectInputStream(file);

                                    // Method for deserialization of object
                                    page = (Page) in.readObject();


                                    System.out.println("Deserialized");

                                    in.close();
                                    file.close();
                                } catch (IOException | ClassNotFoundException ex) {
                                    System.out.println("IOException is caught");
                                }

                                for (int j = 0; j < page.rows.size(); j++)  //loop on the page rows
                                {
                                    Boolean yes = true;
                                    Set<Integer> a = needed.keySet();//index array
                                    for (int k : a) {
                                        Object toBeCompared = ((Vector) page.rows.get(j)).get(k); //get the value of specifies index
                                        ArrayList<Object> ourValue = (ArrayList<Object>) needed.get(k);
                                        if (((String) ourValue.get(0)).equals("java.lang.String")) {
                                            if (!((String) toBeCompared).equals((String) ourValue.get(1))) {
                                                yes = false;
                                                break;//no need to check the rest
                                            }
                                        } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                                            if (!((Integer) toBeCompared).equals((Integer) ourValue.get(1))) {
                                                yes = false;
                                                break;
                                            }
                                        } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                                            if (!((Double) toBeCompared).equals((Double) ourValue.get(1))) {
                                                yes = false;
                                                break;
                                            }
                                        } else if (((String) ourValue.get(0)).equals("java.util.Date")) {
                                            if (!((Date) toBeCompared).equals((Date) ourValue.get(1))) {
                                                yes = false;
                                                break;
                                            }
                                        }
                                    }

                                    if (yes) {
                                        Object primary = PrimaryKey(tableName);
                                        int indexOfPK = getPKIndex(tableName);//now I have the index of PK
                                        Object valueOdPK = ((Vector) page.rows.get(j)).get(indexOfPK);
//                                    System.out.println(valueOdPK);//now I have the value of PK of this raw
                                        page.rows.remove(j);
                                        ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).removeElement(valueOdPK);
                                        System.out.println(((Vector) database.tables.get(indexFound).pages.get(i).get(5)));//remove from the list of keys
                                        database.tables.get(indexFound).ids.removeElement(valueOdPK);
                                    } else {
                                        yes = true;
                                    }

                                    //handle deletion of pages
                                    if (PKType.equals("java.lang.String")) {
                                        if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                        {
                                            //handle delete of pages
                                            if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                            {


                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                    //All its overflow pages should have the new min and max values
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                    String newmin = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                    String newmax = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                    int counter = i + 1;
                                                    while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                        database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                        database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                        counter++;
                                                    }

                                                } else { //case if it is overflow and has overflow !=-1 & true
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                }
                                            }
                                            //if I have no overflows
                                            else {
                                                //and I am an overflow to other page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                    database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                }
                                            }

                                            try {
                                                File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                if (f.delete())                      //returns Boolean value
                                                {
                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                } else {
                                                    System.out.println("failed");
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            database.tables.get(indexFound).pages.remove(i);
                                            i--;
                                        } else {//not empty page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            String newmin2 = (String) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            String newmax2 = (String) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                //do not forget the rest of cases
                                            }
                                            //serialize page
                                            try {
                                                FileOutputStream fileOut =
                                                        new FileOutputStream(path);
                                                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                out.writeObject(page);
                                                out.close();
                                                fileOut.close();

                                            } catch (IOException i2) {
                                                i2.printStackTrace();
                                            }
                                        }

                                        //serialize db
                                        //serilize the database

                                        try {
                                            //Saving of object in a file
                                            String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                            FileOutputStream file = new FileOutputStream(pathdb);
                                            ObjectOutputStream out = new ObjectOutputStream(file);

                                            // Method for serialization of object
                                            out.writeObject(database);

                                            out.close();
                                            file.close();

                                            System.out.println("Object has been serialized");

                                        } catch (IOException ex) {
                                            System.out.println("IOException is caught");
                                        }


                                    } else if (PKType.equals("java.lang.Integer")) {
                                        if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                        {
                                            //handle delete of pages
                                            if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                            {


                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                    //All its overflow pages should have the new min and max values
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                    Integer newmin = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                    Integer newmax = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                    int counter = i + 1;
                                                    while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                        database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                        database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                        counter++;
                                                    }

                                                } else { //case if it is overflow and has overflow !=-1 & true
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                }
                                            }
                                            //if I have no overflows
                                            else {
                                                //and I am an overflow to other page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                    database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                }
                                            }

                                            try {

                                                File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                if (f.delete())                      //returns Boolean value
                                                {
                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                } else {
                                                    System.out.println("failed");
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            database.tables.get(indexFound).pages.remove(i);
                                            i--;
                                        } else {//not empty page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Integer newmin2 = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Integer newmax2 = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                //do not forget the rest of cases
                                            }
                                            //serialize page
                                            try {
                                                FileOutputStream fileOut =
                                                        new FileOutputStream(path);
                                                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                out.writeObject(page);
                                                out.close();
                                                fileOut.close();

                                            } catch (IOException i2) {
                                                i2.printStackTrace();
                                            }
                                        }

                                        //serialize db
                                        //serilize the database

                                        try {
                                            //Saving of object in a file
                                            String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                            FileOutputStream file = new FileOutputStream(pathdb);
                                            ObjectOutputStream out = new ObjectOutputStream(file);

                                            // Method for serialization of object
                                            out.writeObject(database);

                                            out.close();
                                            file.close();

                                            System.out.println("Object has been serialized");

                                        } catch (IOException ex) {
                                            System.out.println("IOException is caught");
                                        }


                                    } else if (PKType.equals("java.lang.Double")) {
                                        if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                        {
                                            //handle delete of pages
                                            if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                            {


                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                    //All its overflow pages should have the new min and max values
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                    Double newmin = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                    Double newmax = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                    int counter = i + 1;
                                                    while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                        database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                        database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                        counter++;
                                                    }

                                                } else { //case if it is overflow and has overflow !=-1 & true
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                }
                                            }
                                            //if I have no overflows
                                            else {
                                                //and I am an overflow to other page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                    database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                }
                                            }

                                            try {
                                                File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                if (f.delete())                      //returns Boolean value
                                                {
                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                } else {
                                                    System.out.println("failed");
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            database.tables.get(indexFound).pages.remove(i);
                                            i--;
                                        } else {//not empty page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Double newmin2 = (Double) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Double newmax2 = (Double) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                //do not forget the rest of cases
                                            }
                                            try {
                                                FileOutputStream fileOut =
                                                        new FileOutputStream(path);
                                                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                out.writeObject(page);
                                                out.close();
                                                fileOut.close();

                                            } catch (IOException i2) {
                                                i2.printStackTrace();
                                            }
                                        }
                                        //serialize page

                                        //serialize db
                                        //serilize the database

                                        try {
                                            //Saving of object in a file
                                            String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                            FileOutputStream file = new FileOutputStream(pathdb);
                                            ObjectOutputStream out = new ObjectOutputStream(file);

                                            // Method for serialization of object
                                            out.writeObject(database);

                                            out.close();
                                            file.close();

                                            System.out.println("Object has been serialized");

                                        } catch (IOException ex) {
                                            System.out.println("IOException is caught");
                                        }


                                    } else if (PKType.equals("java.lang.Date")) {
                                        if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                        {
                                            //handle delete of pages
                                            if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                            {


                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                    //All its overflow pages should have the new min and max values
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                    Date newmin = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                    Date newmax = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                    int counter = i + 1;
                                                    while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                        database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                        database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                        counter++;
                                                    }

                                                } else { //case if it is overflow and has overflow !=-1 & true
                                                    database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                }
                                            }
                                            //if I have no overflows
                                            else {
                                                //and I am an overflow to other page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                    database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                }
                                            }

                                            try {
                                                File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                if (f.delete())                      //returns Boolean value
                                                {
                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                } else {
                                                    System.out.println("failed");
                                                }
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }

                                            database.tables.get(indexFound).pages.remove(i);
                                            i--;
                                        } else {//not empty page
                                            if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Date newmin2 = (Date) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Date newmax2 = (Date) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                //do not forget the rest of cases
                                            }
                                            //serialize page
                                            try {
                                                FileOutputStream fileOut =
                                                        new FileOutputStream(path);
                                                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                out.writeObject(page);
                                                out.close();
                                                fileOut.close();

                                            } catch (IOException i2) {
                                                i2.printStackTrace();
                                            }
                                        }

                                        //serialize db
                                        //serilize the database

                                        try {
                                            //Saving of object in a file
                                            String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                            FileOutputStream file = new FileOutputStream(pathdb);
                                            ObjectOutputStream out = new ObjectOutputStream(file);

                                            // Method for serialization of object
                                            out.writeObject(database);

                                            out.close();
                                            file.close();

                                            System.out.println("Object has been serialized");

                                        } catch (IOException ex) {
                                            System.out.println("IOException is caught");
                                        }


                                    }

                                }

                            }

                        } else {
                            // there are indicessssssssssss
                            boolean foundP = false;
                            int foundC = 0;
                            int diff = columnNameValue.size();
                            String used = "";
                            for (int l = 0; l < database.tables.get(indexFound).indicies.size(); l++) {
                                boolean flag = true;
                                for (int y = 0; y < ((Vector) database.tables.get(indexFound).indicies.get(l).get(1)).size(); y++) {
                                    //checkif alla columns exist
                                    if (!(columnNameValue.containsKey(((Vector) database.tables.get(indexFound).indicies.get(l).get(1)).get(y)))) {
                                        flag = false;
                                        break;
                                    }
                                }
                                if (flag) {
                                    //all the columns may exist or partially
                                    if (columnNameValue.size() == ((Vector) database.tables.get(indexFound).indicies.get(l).get(1)).size()) {
                                        //found the index finalllyyyy
                                        foundP = true;
                                        foundC = l;
                                        diff = 0;
                                        used = ((String) database.tables.get(indexFound).indicies.get(l).get(0));
                                        break;
                                    } else {
                                        //keep track of the indices
                                        if (columnNameValue.size() - ((Vector) database.tables.get(indexFound).indicies.get(l).get(1)).size() < diff) {
                                            //compare
                                            foundP = true;
                                            foundC = l;
                                            diff = columnNameValue.size() - ((Vector) database.tables.get(indexFound).indicies.get(l).get(1)).size();
                                            used = ((String) database.tables.get(indexFound).indicies.get(l).get(0));
                                        }
                                    }
                                }
                            }
                            if (foundP) {
                                //found an indexxxxxxxxxxxxxx
                                if (diff == 0) {
                                    //found a good indexxxx
                                    String pathGrid = (String) (database.tables.get(indexFound).indicies.get(foundC).get(0));
                                    Vector columns = (Vector) (database.tables.get(indexFound).indicies.get(foundC).get(1));
                                    GridIndex grid = null;
                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(pathGrid);
                                        ObjectInputStream in = new ObjectInputStream(file);
                                        // Method for deserialization of object
                                        grid = (GridIndex) in.readObject();
                                        in.close();
                                        file.close();
                                    } catch (IOException ex) {
                                        System.out.println("IOException is caught");
                                    } catch (ClassNotFoundException ex) {
                                        System.out.println("ClassNotFoundException is caught");
                                    }
                                    String id = "";
                                    for (int o = 0; o < columns.size(); o++) {
                                        Object x = columnNameValue.get(columns.get(o));
                                        String dataType = grid.colDataTypes.get(columns.get(o));
                                        if (dataType.equals("java.lang.Integer")) {
                                            int z = binarySearchOnRangesINT((grid.Dimensions.get(o)).arr, Integer.parseInt((String) x));
                                            id += z;
                                        } else if (dataType.equals("java.lang.Double")) {
                                            int z = binarySearchOnRangesDouble((grid.Dimensions.get(o)).arr, Double.parseDouble((String) x));
                                            id += z;
                                        } else if (dataType.equals("java.lang.String")) {
                                            int z = binarySearchOnRangesString((grid.Dimensions.get(o)).arr, (String) x);
                                            id += z;
                                        } else {
                                            int z = binarySearchOnRangesDate((grid.Dimensions.get(o)).arr, parseDate((String) x));
                                            id += z;
                                        }
                                    }
                                    System.out.println(id);
                                    //got the idddddd

                                    for (int o = 0; o < grid.pathOfBuckets.size(); o++) {
                                        if (((String) (grid.pathOfBuckets.get(o)).get(0)).equals(id)) {
                                            //found the pathss
                                            Vector ids = new Vector();
                                            for (int t = 0; t < ((Vector) (grid.pathOfBuckets.get(o)).get(1)).size(); t++) {
                                                Bucket bucket1 = null;
                                                try {
                                                    // Reading the object from a file
                                                    FileInputStream file = new FileInputStream(((String) ((Vector) (grid.pathOfBuckets.get(o)).get(1)).get(t)));
                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                    // Method for deserialization of object
                                                    bucket1 = (Bucket) in.readObject();
                                                    in.close();
                                                    file.close();
                                                } catch (IOException ex) {
                                                    System.out.println("IOException is caught");
                                                } catch (ClassNotFoundException ex) {
                                                    System.out.println("ClassNotFoundException is caught");
                                                }
                                                //NOWWWW PAGESSSSS
                                                for (int e = 0; e < bucket1.Items.size(); e++) {
                                                    Page page9 = null;
                                                    String pathBucket = (String) bucket1.Items.get(e).get(1);
                                                    try {
                                                        // Reading the object from a file
                                                        FileInputStream file = new FileInputStream(pathBucket);
                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                        // Method for deserialization of object
                                                        page9 = (Page) in.readObject();
                                                        in.close();
                                                        file.close();
                                                    } catch (IOException ex) {
                                                        System.out.println("IOException is caught");
                                                    } catch (ClassNotFoundException ex) {
                                                        System.out.println("ClassNotFoundException is caught");
                                                    }
                                                    for (int u = 0; u < database.tables.get(indexFound).pages.size(); u++) {
                                                        if ((((HashMap) database.tables.get(indexFound).pages.get(u)).get(0)).equals(pathBucket)) {
                                                            int index = -1;
                                                            if (PKType.equals("java.lang.Double")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                index = binarySearchDouble(a, 0, a.size() - 1, (Double) bucket1.Items.get(e).get(0));

                                                            } else if (PKType.equals("java.lang.String")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                index = binarySearchString(a, 0, a.size() - 1, (String) bucket1.Items.get(e).get(0));

                                                            } else if (PKType.equals("java.util.Date")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                System.out.println();
                                                                index = binarySearchDate(a, 0, a.size() - 1, (Date) bucket1.Items.get(e).get(0));
                                                            } else if (PKType.equals("java.lang.Integer")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                index = binarySearchInteger(a, 0, a.size() - 1, (Integer) bucket1.Items.get(e).get(0));
                                                            }
                                                            /////lesaaaaaaaaa hna
                                                            HashMap<Integer, Object> needed = theArrNeeded(columnNameValue, dataTypes);//indices is the key and [datatypes,value]
                                                            Set<Integer> a = needed.keySet();//index array
                                                            System.out.println(needed.toString());
                                                            //   System.out.println(needed.toString());
                                                            Boolean yes = true;
                                                            for (int k : a) {
                                                                Object toBeCompared = ((Vector) page9.rows.get(index)).get(k); //get the value of specifies index
                                                                ArrayList<Object> ourValue = (ArrayList<Object>) needed.get(k);
                                                                System.out.println(ourValue);
                                                                System.out.println(toBeCompared);
                                                                if (((String) ourValue.get(0)).equals("java.lang.String")) {
                                                                    if (!((String) toBeCompared).equals((String) ourValue.get(1))) {
                                                                        yes = false;
                                                                        break;//no need to check the rest
                                                                    }
                                                                } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                                                                    if (!((Integer) toBeCompared).equals(Integer.parseInt((String) ourValue.get(1)))) {
                                                                        yes = false;
                                                                        break;
                                                                    }
                                                                } else if (((String) ourValue.get(0)).equals("java.lang.Double")) {
                                                                    if (!((Double) toBeCompared).equals(Double.parseDouble((String) ourValue.get(1)))) {
                                                                        yes = false;
                                                                        break;
                                                                    }
                                                                } else if (((String) ourValue.get(0)).equals("java.util.Date")) {

                                                                    if (!((Date) toBeCompared).equals(parseDate((String) ourValue.get(1)))) {
                                                                        yes = false;
                                                                        break;
                                                                    }
                                                                }


                                                            }
                                                            System.out.println(pathBucket);
                                                            System.out.println("Entered");
                                                            if (yes) {
                                                                System.out.println("USEEEEEEEEEEEEED");
                                                                ids.add(bucket1.Items.get(e).get(0));
                                                                System.out.println(bucket1.Items.get(e));
                                                                bucket1.Items.removeElementAt(e);
                                                                if (bucket1.Items.isEmpty()) {
                                                                    //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                    try {
                                                                        File f = new File((String) ((Vector) (grid.pathOfBuckets.get(o)).get(1)).get(t));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e1) {
                                                                        e1.printStackTrace();
                                                                    }
                                                                    ((Vector) (grid.pathOfBuckets.get(o).get(1))).removeElementAt(t);
                                                                    if (((Vector) (grid.pathOfBuckets.get(o).get(1))).isEmpty()) {
                                                                        //removed the id
                                                                        grid.pathOfBuckets.remove(o);
                                                                        grid.bucketIDs.remove(id);
                                                                        System.out.println(pathGrid);
                                                                        System.out.println(grid.pathOfBuckets);
                                                                    } else {
                                                                        if (t > 0) {
                                                                            String path10 = ((String) ((Vector) (grid.pathOfBuckets.get(o)).get(0)).get(t - 1));
                                                                            Bucket bucket5 = null;
                                                                            try {
                                                                                // Reading the object from a file
                                                                                FileInputStream file = new FileInputStream(path10);
                                                                                ObjectInputStream in = new ObjectInputStream(file);
                                                                                // Method for deserialization of object
                                                                                bucket5 = (Bucket) in.readObject();
                                                                                in.close();
                                                                                file.close();
                                                                            } catch (IOException ex) {
                                                                                System.out.println("IOException is caught");
                                                                            } catch (ClassNotFoundException ex) {
                                                                                System.out.println("ClassNotFoundException is caught");
                                                                            }
                                                                            bucket5.hasOverflow = bucket1.hasOverflow;
                                                                            bucket5.pathOfOverflows = bucket1.pathOfOverflows;
                                                                        } else {
                                                                            String path10 = ((String) ((Vector) (grid.pathOfBuckets.get(o)).get(0)).get(t + 1));
                                                                            Bucket bucket5 = null;
                                                                            try {
                                                                                // Reading the object from a file
                                                                                FileInputStream file = new FileInputStream(path10);
                                                                                ObjectInputStream in = new ObjectInputStream(file);
                                                                                // Method for deserialization of object
                                                                                bucket5 = (Bucket) in.readObject();
                                                                                in.close();
                                                                                file.close();
                                                                            } catch (IOException ex) {
                                                                                System.out.println("IOException is caught");
                                                                            } catch (ClassNotFoundException ex) {
                                                                                System.out.println("ClassNotFoundException is caught");
                                                                            }
                                                                            bucket5.overflow = false;
                                                                        }

                                                                    }
                                                                    grid.ser(pathGrid);

                                                                } else {
                                                                    bucket1.ser(((String) ((Vector) (grid.pathOfBuckets.get(o)).get(1)).get(t)));
                                                                    e--;
                                                                }
                                                                grid.ser(pathGrid);
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                                if (!grid.bucketIDs.contains(id)) {
                                                    break;
                                                }
                                            }
                                            //got the idss
                                            for (int r = 0; r < ids.size(); r++) {
                                                System.out.println("with idssssssss");
                                                System.out.println(ids.toString());
                                                //now loop and delete
                                                for (int h = 0; h < database.tables.get(indexFound).indicies.size(); h++) {
                                                    //n4eel ba2a l ids mn l indices
                                                    System.out.println("elhamdullah f tani tagrobaaaa");
                                                    String currentGrid = ((String) database.tables.get(indexFound).indicies.get(h).get(0));
                                                    System.out.println(used);
                                                    System.out.println(currentGrid);
                                                    if (!currentGrid.equals(used)) {
                                                        GridIndex currentGridToDelete = null;
                                                        String path3 = (String) database.tables.get(indexFound).indicies.get(h).get(0);
                                                        try {
                                                            // Reading the object from a file
                                                            FileInputStream file = new FileInputStream(path3);
                                                            ObjectInputStream in = new ObjectInputStream(file);

                                                            // Method for deserialization of object
                                                            currentGridToDelete = (GridIndex) in.readObject();


                                                            System.out.println("Deserialized");

                                                            in.close();
                                                            file.close();
                                                        } catch (IOException | ClassNotFoundException ex) {
                                                            System.out.println("IOException is caught");
                                                        }
                                                        System.out.println("entering to the indicesssss");
                                                        for (int n = 0; n < currentGridToDelete.pathOfBuckets.size(); n++) {
                                                            System.out.println("entered indicesssssssss ");

                                                            for (int c = 0; c < ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).size(); c++) {
                                                                Bucket currentBucketToDelete = null;
                                                                boolean flag = false ;
                                                                String path1 = (String) ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).get(c);
                                                                System.out.println(path1);
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path1);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    currentBucketToDelete = (Bucket) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                for (int b = 0; b < currentBucketToDelete.Items.size(); b++) {
                                                                    System.out.println(currentBucketToDelete.Items.get(b));
                                                                    if (PKType.equals("java.lang.Double")) {
                                                                        if (((Double) (currentBucketToDelete.Items.get(b)).get(0)).equals((Double) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                System.out.println("Wnabiii ba2aa zh2ttttt");
                                                                                System.out.println(((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).get(c));
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    System.out.println(id1);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    System.out.println(currentGridToDelete.bucketIDs.toString());
                                                                                    System.out.println(currentGridToDelete.pathOfBuckets.toString());
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);
                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    } else if (PKType.equals("java.lang.String")) {
                                                                        if (((String) (currentBucketToDelete.Items.get(b)).get(0)).equals((String) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    } else if (PKType.equals("java.util.Date")) {
                                                                        if (((Date) (currentBucketToDelete.Items.get(b)).get(0)).equals((Date) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    } else if (PKType.equals("java.lang.Integer")) {
                                                                        if (((Integer) (currentBucketToDelete.Items.get(b)).get(0)).equals((Integer) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    }


                                                                }
                                                                if(flag)
                                                                    break;
                                                            }


                                                        }


                                                    }


                                                }
//                                                ArrayList<Object> pkNeeded = pkUsed(columnNameValue, dataTypes);
//                                                System.out.println(pkNeeded);
                                                for (int i = 0; i < database.tables.get(indexFound).pages.size(); i++) {
                                                    String pktype = PKType;
                                                    Object pk = ids.get(r);
                                                    if (pktype.equals("java.lang.String")) {
                                                        String min = (String) database.tables.get(indexFound).pages.get(i).get(3);
                                                        String max = (String) database.tables.get(indexFound).pages.get(i).get(4);
                                                        if (((String) pk).compareTo(min) >= 0 && ((String) pk).compareTo(max) <= 0
                                                                || ((String) pk).compareTo((String) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {

                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (String) pk);

                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            String newmin = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            String newmax = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                            }


                                                                        //do not forget the rest of cases
                                                                    }
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }
                                                                //serialize page

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;
                                                            }

                                                        }

                                                    } else if (pktype.equals("java.lang.Integer")) {
                                                        if ((((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                                                ((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                                                || ((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Integer) pk);
                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;

                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            int newmin = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            int newmax = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
                                                                        //do not forget the rest of cases
                                                                    }
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }
                                                                //serialize page

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;

                                                            }

                                                        }
                                                    } else if (pktype.equals("java.lang.Double")) {
                                                        if ((((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                                                ((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                                                || ((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Double) pk);
                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;

                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            Double newmin = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            Double newmax = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
                                                                        //update the min and max of the curr page and all its overflows if I am original


                                                                        //do not forget the rest of cases
                                                                    }
                                                                    //serialize page
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;

                                                            }

                                                        }
                                                    } else if (pktype.equals("java.util.Date")) {
                                                        if ((((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                                                ((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                                                || ((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Date) pk);
                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;

                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
//                                    ((Vector)database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            Date newmin = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            Date newmax = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows

                                                                        //do not forget the rest of cases
                                                                    }
                                                                    //serialize page
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;

                                                            }

                                                        }
                                                    }

                                                }

                                            }
                                            break;
                                        }
                                    }
                                } else {
                                    //found not a good onee
                                    String pathGrid = (String) (database.tables.get(indexFound).indicies.get(foundC).get(0));
                                    Vector columns = (Vector) (database.tables.get(indexFound).indicies.get(foundC).get(1));
                                    GridIndex grid = null;
                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(pathGrid);
                                        ObjectInputStream in = new ObjectInputStream(file);
                                        // Method for deserialization of object
                                        grid = (GridIndex) in.readObject();
                                        in.close();
                                        file.close();
                                    } catch (IOException ex) {
                                        System.out.println("IOException is caught");
                                    } catch (ClassNotFoundException ex) {
                                        System.out.println("ClassNotFoundException is caught");
                                    }
                                    String id = "";
                                    for (int o = 0; o < columns.size(); o++) {
                                        Object x = columnNameValue.get(columns.get(o));
                                        String dataType = grid.colDataTypes.get(columns.get(o));
                                        if (dataType.equals("java.lang.Integer")) {
                                            int z = binarySearchOnRangesINT((grid.Dimensions.get(o)).arr, Integer.parseInt((String) x));
                                            id += z;
                                        } else if (dataType.equals("java.lang.Double")) {
                                            int z = binarySearchOnRangesDouble((grid.Dimensions.get(o)).arr, Double.parseDouble((String) x));
                                            id += z;
                                        } else if (dataType.equals("java.lang.String")) {
                                            int z = binarySearchOnRangesString((grid.Dimensions.get(o)).arr, (String) x);
                                            id += z;
                                        } else {
                                            int z = binarySearchOnRangesDate((grid.Dimensions.get(o)).arr, parseDate((String) x));
                                            id += z;
                                        }
                                    }
                                    System.out.println(id);
                                    //got the idddddd

                                    for (int o = 0; o < grid.pathOfBuckets.size(); o++) {
                                        if (((String) (grid.pathOfBuckets.get(o)).get(0)).equals(id)) {
                                            //found the pathss
                                            Vector ids = new Vector();
                                            for (int t = 0; t < ((Vector) (grid.pathOfBuckets.get(o)).get(1)).size(); t++) {
                                                Bucket bucket1 = null;
                                                try {
                                                    // Reading the object from a file
                                                    FileInputStream file = new FileInputStream(((String) ((Vector) (grid.pathOfBuckets.get(o)).get(1)).get(t)));
                                                    ObjectInputStream in = new ObjectInputStream(file);
                                                    // Method for deserialization of object
                                                    bucket1 = (Bucket) in.readObject();
                                                    in.close();
                                                    file.close();
                                                } catch (IOException ex) {
                                                    System.out.println("IOException is caught");
                                                } catch (ClassNotFoundException ex) {
                                                    System.out.println("ClassNotFoundException is caught");
                                                }
                                                //NOWWWW PAGESSSSS
                                                for (int e = 0; e < bucket1.Items.size(); e++) {
                                                    Page page9 = null;
                                                    String pathBucket = (String) bucket1.Items.get(e).get(1);
                                                    try {
                                                        // Reading the object from a file
                                                        FileInputStream file = new FileInputStream(pathBucket);
                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                        // Method for deserialization of object
                                                        page9 = (Page) in.readObject();
                                                        in.close();
                                                        file.close();
                                                    } catch (IOException ex) {
                                                        System.out.println("IOException is caught");
                                                    } catch (ClassNotFoundException ex) {
                                                        System.out.println("ClassNotFoundException is caught");
                                                    }
                                                    for (int u = 0; u < database.tables.get(indexFound).pages.size(); u++) {
                                                        if ((((HashMap) database.tables.get(indexFound).pages.get(u)).get(0)).equals(pathBucket)) {
                                                            int index = -1;
                                                            if (PKType.equals("java.lang.Double")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                index = binarySearchDouble(a, 0, a.size() - 1, (Double) bucket1.Items.get(e).get(0));

                                                            } else if (PKType.equals("java.lang.String")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                index = binarySearchString(a, 0, a.size() - 1, (String) bucket1.Items.get(e).get(0));

                                                            } else if (PKType.equals("java.util.Date")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                System.out.println();
                                                                index = binarySearchDate(a, 0, a.size() - 1, (Date) bucket1.Items.get(e).get(0));
                                                            } else if (PKType.equals("java.lang.Integer")) {
                                                                Vector a = (Vector) (((HashMap) database.tables.get(indexFound).pages.get(u)).get(5));
                                                                index = binarySearchInteger(a, 0, a.size() - 1, (Integer) bucket1.Items.get(e).get(0));
                                                            }
                                                            /////lesaaaaaaaaa hna
                                                            HashMap<Integer, Object> needed = theArrNeeded(columnNameValue, dataTypes);//indices is the key and [datatypes,value]
                                                            Set<Integer> a = needed.keySet();//index array
                                                            System.out.println(needed.toString());
                                                            //   System.out.println(needed.toString());
                                                            Boolean yes = true;
                                                            for (int k : a) {
                                                                Object toBeCompared = ((Vector) page9.rows.get(index)).get(k); //get the value of specifies index
                                                                ArrayList<Object> ourValue = (ArrayList<Object>) needed.get(k);
                                                                System.out.println(ourValue);
                                                                System.out.println(toBeCompared);
                                                                if (((String) ourValue.get(0)).equals("java.lang.String")) {
                                                                    if (!((String) toBeCompared).equals((String) ourValue.get(1))) {
                                                                        yes = false;
                                                                        break;//no need to check the rest
                                                                    }
                                                                } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                                                                    if (!((Integer) toBeCompared).equals(Integer.parseInt((String) ourValue.get(1)))) {
                                                                        yes = false;
                                                                        break;
                                                                    }
                                                                } else if (((String) ourValue.get(0)).equals("java.lang.Double")) {
                                                                    if (!((Double) toBeCompared).equals(Double.parseDouble((String) ourValue.get(1)))) {
                                                                        yes = false;
                                                                        break;
                                                                    }
                                                                } else if (((String) ourValue.get(0)).equals("java.util.Date")) {

                                                                    if (!((Date) toBeCompared).equals(parseDate((String) ourValue.get(1)))) {
                                                                        yes = false;
                                                                        break;
                                                                    }
                                                                }


                                                            }
                                                            System.out.println(pathBucket);
                                                            System.out.println("Entered");
                                                            if (yes) {
                                                                System.out.println("USEEEEEEEEEEEEED");
                                                                ids.add(bucket1.Items.get(e).get(0));
                                                                System.out.println(bucket1.Items.get(e));
                                                                bucket1.Items.removeElementAt(e);
                                                                if (bucket1.Items.isEmpty()) {
                                                                    //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                    try {
                                                                        File f = new File((String) ((Vector) (grid.pathOfBuckets.get(o)).get(1)).get(t));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e1) {
                                                                        e1.printStackTrace();
                                                                    }
                                                                    ((Vector) (grid.pathOfBuckets.get(o).get(1))).removeElementAt(t);
                                                                    if (((Vector) (grid.pathOfBuckets.get(o).get(1))).isEmpty()) {
                                                                        //removed the id
                                                                        grid.pathOfBuckets.remove(o);
                                                                        grid.bucketIDs.remove(id);
                                                                        grid.ser(pathGrid);
                                                                    } else {
                                                                        if (t > 0) {
                                                                            String path10 = ((String) ((Vector) (grid.pathOfBuckets.get(o)).get(0)).get(t - 1));
                                                                            Bucket bucket5 = null;
                                                                            try {
                                                                                // Reading the object from a file
                                                                                FileInputStream file = new FileInputStream(path10);
                                                                                ObjectInputStream in = new ObjectInputStream(file);
                                                                                // Method for deserialization of object
                                                                                bucket5 = (Bucket) in.readObject();
                                                                                in.close();
                                                                                file.close();
                                                                            } catch (IOException ex) {
                                                                                System.out.println("IOException is caught");
                                                                            } catch (ClassNotFoundException ex) {
                                                                                System.out.println("ClassNotFoundException is caught");
                                                                            }
                                                                            bucket5.hasOverflow = bucket1.hasOverflow;
                                                                            bucket5.pathOfOverflows = bucket1.pathOfOverflows;
                                                                        } else {
                                                                            String path10 = ((String) ((Vector) (grid.pathOfBuckets.get(o)).get(0)).get(t + 1));
                                                                            Bucket bucket5 = null;
                                                                            try {
                                                                                // Reading the object from a file
                                                                                FileInputStream file = new FileInputStream(path10);
                                                                                ObjectInputStream in = new ObjectInputStream(file);
                                                                                // Method for deserialization of object
                                                                                bucket5 = (Bucket) in.readObject();
                                                                                in.close();
                                                                                file.close();
                                                                            } catch (IOException ex) {
                                                                                System.out.println("IOException is caught");
                                                                            } catch (ClassNotFoundException ex) {
                                                                                System.out.println("ClassNotFoundException is caught");
                                                                            }
                                                                            bucket5.overflow = false;
                                                                        }

                                                                    }
                                                                    grid.ser(pathGrid);

                                                                } else {
                                                                    bucket1.ser(((String) ((Vector) (grid.pathOfBuckets.get(o)).get(1)).get(t)));
                                                                    e--;
                                                                }
                                                                break;
                                                            }

                                                        }
                                                    }
                                                }
                                                if (!grid.bucketIDs.contains(id)) {
                                                    break;
                                                }
                                            }
                                            //got the idss
                                            for (int r = 0; r < ids.size(); r++) {
                                                //now loop and delete
                                                for (int h = 0; h < database.tables.get(indexFound).indicies.size(); h++) {
                                                    //n4eel ba2a l ids mn l indices
                                                    System.out.println("elhamdullah f tani tagrobaaaa");
                                                    String currentGrid = ((String) database.tables.get(indexFound).indicies.get(h).get(0));
                                                    System.out.println(used);
                                                    System.out.println(currentGrid);
                                                    if (!currentGrid.equals(used)) {
                                                        GridIndex currentGridToDelete = null;
                                                        String path3 = (String) database.tables.get(indexFound).indicies.get(h).get(0);
                                                        try {
                                                            // Reading the object from a file
                                                            FileInputStream file = new FileInputStream(path3);
                                                            ObjectInputStream in = new ObjectInputStream(file);

                                                            // Method for deserialization of object
                                                            currentGridToDelete = (GridIndex) in.readObject();


                                                            System.out.println("Deserialized");

                                                            in.close();
                                                            file.close();
                                                        } catch (IOException | ClassNotFoundException ex) {
                                                            System.out.println("IOException is caught");
                                                        }
                                                        System.out.println("entering to the indicesssss");
                                                        for (int n = 0; n < currentGridToDelete.pathOfBuckets.size(); n++) {
                                                            System.out.println("entered indicesssssssss ");

                                                            for (int c = 0; c < ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).size(); c++) {
                                                                Bucket currentBucketToDelete = null;
                                                                boolean flag = false ;
                                                                String path1 = (String) ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).get(c);
                                                                System.out.println(path1);
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path1);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    currentBucketToDelete = (Bucket) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                for (int b = 0; b < currentBucketToDelete.Items.size(); b++) {
                                                                    System.out.println(currentBucketToDelete.Items.get(b));
                                                                    if (PKType.equals("java.lang.Double")) {
                                                                        if (((Double) (currentBucketToDelete.Items.get(b)).get(0)).equals((Double) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                System.out.println("Wnabiii ba2aa zh2ttttt");
                                                                                System.out.println(((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).get(c));
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    System.out.println(id1);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    System.out.println(currentGridToDelete.bucketIDs.toString());
                                                                                    System.out.println(currentGridToDelete.pathOfBuckets.toString());
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    } else if (PKType.equals("java.lang.String")) {
                                                                        if (((String) (currentBucketToDelete.Items.get(b)).get(0)).equals((String) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    } else if (PKType.equals("java.util.Date")) {
                                                                        if (((Date) (currentBucketToDelete.Items.get(b)).get(0)).equals((Date) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                System.out.println("Wnabiii ba2aa zh2ttttt");
                                                                                System.out.println(((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).get(c));
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    System.out.println(id1);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    System.out.println(currentGridToDelete.bucketIDs.toString());
                                                                                    System.out.println(currentGridToDelete.pathOfBuckets.toString());
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    } else if (PKType.equals("java.lang.Integer")) {
                                                                        if (((Integer) (currentBucketToDelete.Items.get(b)).get(0)).equals((Integer) ids.get(r))) {
                                                                            //delete here
                                                                            flag = true ;
                                                                            currentBucketToDelete.Items.removeElementAt(b);
                                                                            if (currentBucketToDelete.Items.isEmpty()) {
                                                                                //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                                try {
                                                                                    File f = new File(path1);           //file to be delete
                                                                                    if (f.delete())                      //returns Boolean value
                                                                                    {
                                                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                    } else {
                                                                                        System.out.println("failed");
                                                                                    }
                                                                                } catch (Exception e1) {
                                                                                    e1.printStackTrace();
                                                                                }
                                                                                ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                                if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                    //removed the id
                                                                                    String id1 = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                    currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                    currentGridToDelete.bucketIDs.removeElement(id1);
                                                                                    currentGridToDelete.ser(currentGrid);
                                                                                    break;
                                                                                } else {
                                                                                    if (c > 0) {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                        bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                        bucket5.ser(path10);
                                                                                    } else {
                                                                                        String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                        Bucket bucket5 = null;
                                                                                        try {
                                                                                            // Reading the object from a file
                                                                                            FileInputStream file = new FileInputStream(path10);
                                                                                            ObjectInputStream in = new ObjectInputStream(file);
                                                                                            // Method for deserialization of object
                                                                                            bucket5 = (Bucket) in.readObject();
                                                                                            in.close();
                                                                                            file.close();
                                                                                        } catch (IOException ex) {
                                                                                            System.out.println("IOException is caught");
                                                                                        } catch (ClassNotFoundException ex) {
                                                                                            System.out.println("ClassNotFoundException is caught");
                                                                                        }
                                                                                        bucket5.overflow = false;
                                                                                        bucket5.idOfOverflow = null;
                                                                                        bucket5.ser(path10);
                                                                                    }

                                                                                }
                                                                                currentGridToDelete.ser(currentGrid);

                                                                            } else {
                                                                                currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                                b--;
                                                                            }
                                                                            break;
                                                                        }
                                                                    }


                                                                }

                                                                if(flag)
                                                                    break;
                                                            }


                                                        }


                                                    }


                                                }
//                                                ArrayList<Object> pkNeeded = pkUsed(columnNameValue, dataTypes);
//                                                System.out.println(pkNeeded);
                                                for (int i = 0; i < database.tables.get(indexFound).pages.size(); i++) {
                                                    String pktype = PKType;
                                                    Object pk = ids.get(r);
                                                    if (pktype.equals("java.lang.String")) {
                                                        String min = (String) database.tables.get(indexFound).pages.get(i).get(3);
                                                        String max = (String) database.tables.get(indexFound).pages.get(i).get(4);
                                                        if (((String) pk).compareTo(min) >= 0 && ((String) pk).compareTo(max) <= 0
                                                                || ((String) pk).compareTo((String) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {

                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (String) pk);

                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;
                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            String newmin = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            String newmax = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                            }


                                                                        //do not forget the rest of cases
                                                                    }
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }
                                                                //serialize page

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;
                                                            }

                                                        }

                                                    } else if (pktype.equals("java.lang.Integer")) {
                                                        if ((((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                                                ((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                                                || ((Integer) pk).compareTo((Integer) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Integer) pk);
                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;

                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            int newmin = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            int newmax = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
                                                                        //do not forget the rest of cases
                                                                    }
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }
                                                                //serialize page

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;

                                                            }

                                                        }
                                                    } else if (pktype.equals("java.lang.Double")) {
                                                        if ((((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                                                ((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                                                || ((Double) pk).compareTo((Double) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Double) pk);
                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;

                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            Double newmin = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            Double newmax = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
                                                                        //update the min and max of the curr page and all its overflows if I am original


                                                                        //do not forget the rest of cases
                                                                    }
                                                                    //serialize page
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }

                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;

                                                            }

                                                        }
                                                    } else if (pktype.equals("java.util.Date")) {
                                                        if ((((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(3)) >= 0 &&
                                                                ((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(4)) <= 0)
                                                                || ((Date) pk).compareTo((Date) database.tables.get(indexFound).pages.get(i).get(3)) <= 0) {
                                                            int here = Collections.binarySearch((Vector) database.tables.get(indexFound).pages.get(i).get(5), (Date) pk);
                                                            if (here > -1) {
                                                                ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                                                Page page = null;

                                                                try {
                                                                    // Reading the object from a file
                                                                    FileInputStream file = new FileInputStream(path);
                                                                    ObjectInputStream in = new ObjectInputStream(file);

                                                                    // Method for deserialization of object
                                                                    page = (Page) in.readObject();


                                                                    System.out.println("Deserialized");

                                                                    in.close();
                                                                    file.close();
                                                                } catch (IOException | ClassNotFoundException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }
                                                                page.rows.remove(here);
                                                                database.tables.get(indexFound).ids.removeElement(pk);
//                                    ((Vector)database.tables.get(indexFound).pages.get(i).get(5)).remove(here);
                                                                if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                                                {
                                                                    //handle delete of pages
                                                                    if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                                    {


                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                                            //All its overflow pages should have the new min and max values
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                                            Date newmin = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                                            Date newmax = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                                            int counter = i + 1;
                                                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                                                database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                                                database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                                                counter++;
                                                                            }

                                                                        } else { //case if it is overflow and has overflow !=-1 & true
                                                                            database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                                        }
                                                                    }
                                                                    //if I have no overflows
                                                                    else {
                                                                        //and I am an overflow to other page
                                                                        if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                                            database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                                        }
                                                                    }

                                                                    try {
                                                                        File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                                        if (f.delete())                      //returns Boolean value
                                                                        {
                                                                            System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                        } else {
                                                                            System.out.println("failed");
                                                                        }
                                                                    } catch (Exception e) {
                                                                        e.printStackTrace();
                                                                    }

                                                                    database.tables.get(indexFound).pages.remove(i);
                                                                    i--;
                                                                } else {//not empty page
                                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
                                                                        database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows

                                                                        //do not forget the rest of cases
                                                                    }
                                                                    //serialize page
                                                                    try {
                                                                        FileOutputStream fileOut =
                                                                                new FileOutputStream(path);
                                                                        ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                                        out.writeObject(page);
                                                                        out.close();
                                                                        fileOut.close();

                                                                    } catch (IOException i2) {
                                                                        i2.printStackTrace();
                                                                    }
                                                                }


                                                                //serialize db
                                                                //serilize the database

                                                                try {
                                                                    //Saving of object in a file
                                                                    String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                                    FileOutputStream file = new FileOutputStream(pathdb);
                                                                    ObjectOutputStream out = new ObjectOutputStream(file);

                                                                    // Method for serialization of object
                                                                    out.writeObject(database);

                                                                    out.close();
                                                                    file.close();

                                                                    System.out.println("Object has been serialized");

                                                                } catch (IOException ex) {
                                                                    System.out.println("IOException is caught");
                                                                }


                                                                break;

                                                            }

                                                        }
                                                    }

                                                }

                                            }
                                            break;
                                        }


                                    }


                                }


                            } else {
                                //no indexxxx elhamdullah f search 3adii

                                HashMap<Integer, Object> needed = theArrNeeded(columnNameValue, dataTypes);//indices is the key and [datatypes,value]
                                for (int i = 0; i < database.tables.get(indexFound).pages.size(); i++) {
                                    String path = (String) database.tables.get(indexFound).pages.get(i).get(0);
                                    Page page = null;

//                       deserilaze the page
                                    try {
                                        // Reading the object from a file
                                        FileInputStream file = new FileInputStream(path);
                                        ObjectInputStream in = new ObjectInputStream(file);

                                        // Method for deserialization of object
                                        page = (Page) in.readObject();


                                        System.out.println("Deserialized");

                                        in.close();
                                        file.close();
                                    } catch (IOException | ClassNotFoundException ex) {
                                        System.out.println("IOException is caught");
                                    }

                                    for (int j = 0; j < page.rows.size(); j++)  //loop on the page rows
                                    {
                                        Boolean yes = true;
                                        Set<Integer> a = needed.keySet();//index array
                                        for (int k : a) {
                                            Object toBeCompared = ((Vector) page.rows.get(j)).get(k); //get the value of specifies index
                                            ArrayList<Object> ourValue = (ArrayList<Object>) needed.get(k);
                                            if (((String) ourValue.get(0)).equals("java.lang.String")) {
                                                if (!((String) toBeCompared).equals((String) ourValue.get(1))) {
                                                    yes = false;
                                                    break;//no need to check the rest
                                                }
                                            } else if (((String) ourValue.get(0)).equals("java.lang.Integer")) {
                                                if (!((Integer) toBeCompared).equals(Integer.parseInt((String) ourValue.get(1)))) {
                                                    yes = false;
                                                    break;
                                                }
                                            } else if (((String) ourValue.get(0)).equals("java.lang.Double")) {
                                                if (!((Double) toBeCompared).equals(Double.parseDouble((String) ourValue.get(1)))) {
                                                    yes = false;
                                                    break;
                                                }
                                            } else if (((String) ourValue.get(0)).equals("java.util.Date")) {
                                                if (!((Date) toBeCompared).equals(parseDate((String) ourValue.get(1)))) {
                                                    yes = false;
                                                    break;
                                                }
                                            }
                                        }

                                        if (yes) {
                                            Object primary = PrimaryKey(tableName);
                                            int indexOfPK = getPKIndex(tableName);//now I have the index of PK
                                            Object valueOdPK = ((Vector) page.rows.get(j)).get(indexOfPK);
                                            for (int h = 0; h < database.tables.get(indexFound).indicies.size(); h++) {
                                                //n4eel ba2a l ids mn l indices
                                                System.out.println("elhamdullah f tani tagrobaaaa");
                                                String currentGrid = ((String) database.tables.get(indexFound).indicies.get(h).get(0));
                                                System.out.println(used);
                                                System.out.println(currentGrid);
                                                if (!currentGrid.equals(used)) {
                                                    GridIndex currentGridToDelete = null;
                                                    String path3 = (String) database.tables.get(indexFound).indicies.get(h).get(0);
                                                    try {
                                                        // Reading the object from a file
                                                        FileInputStream file = new FileInputStream(path3);
                                                        ObjectInputStream in = new ObjectInputStream(file);

                                                        // Method for deserialization of object
                                                        currentGridToDelete = (GridIndex) in.readObject();


                                                        System.out.println("Deserialized");

                                                        in.close();
                                                        file.close();
                                                    } catch (IOException | ClassNotFoundException ex) {
                                                        System.out.println("IOException is caught");
                                                    }
                                                    System.out.println("entering to the indicesssss");
                                                    for (int n = 0; n < currentGridToDelete.pathOfBuckets.size(); n++) {
                                                        System.out.println("entered indicesssssssss ");

                                                        for (int c = 0; c < ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).size(); c++) {
                                                            Bucket currentBucketToDelete = null;
                                                            boolean flag = false ;
                                                            String path1 = (String) ((Vector) ((currentGridToDelete.pathOfBuckets.get(n)).get(1))).get(c);
                                                            System.out.println(path1);
                                                            try {
                                                                // Reading the object from a file
                                                                FileInputStream file = new FileInputStream(path1);
                                                                ObjectInputStream in = new ObjectInputStream(file);

                                                                // Method for deserialization of object
                                                                currentBucketToDelete = (Bucket) in.readObject();


                                                                System.out.println("Deserialized");

                                                                in.close();
                                                                file.close();
                                                            } catch (IOException | ClassNotFoundException ex) {
                                                                System.out.println("IOException is caught");
                                                            }
                                                            for (int b = 0; b < currentBucketToDelete.Items.size(); b++) {
                                                                System.out.println(currentBucketToDelete.Items.get(b));
                                                                System.out.println(valueOdPK);
                                                                System.out.println((currentBucketToDelete.Items.get(b)).get(0));
                                                                if (PKType.equals("java.lang.Double")) {
                                                                    if (((Double) (currentBucketToDelete.Items.get(b)).get(0)).equals((Double) valueOdPK)) {
                                                                        //delete here
                                                                        flag = true ;
                                                                        currentBucketToDelete.Items.removeElementAt(b);
                                                                        if (currentBucketToDelete.Items.isEmpty()) {
                                                                            //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                            try {
                                                                                File f = new File(path1);           //file to be delete
                                                                                if (f.delete())                      //returns Boolean value
                                                                                {
                                                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                } else {
                                                                                    System.out.println("failed");
                                                                                }
                                                                            } catch (Exception e1) {
                                                                                e1.printStackTrace();
                                                                            }
                                                                            ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                            if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                //removed the id
                                                                                String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                currentGridToDelete.bucketIDs.removeElement(id);
                                                                                currentGridToDelete.ser(currentGrid);
                                                                                break;
                                                                            } else {
                                                                                if (c > 0) {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                    bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                    bucket5.ser(path10);
                                                                                } else {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.overflow = false;
                                                                                    bucket5.idOfOverflow = null;
                                                                                    bucket5.ser(path10);
                                                                                }

                                                                            }
                                                                            currentGridToDelete.ser(currentGrid);

                                                                        } else {
                                                                            currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                            b--;
                                                                        }
                                                                        break;
                                                                    }
                                                                } else if (PKType.equals("java.lang.String")) {
                                                                    if (((String) (currentBucketToDelete.Items.get(b)).get(0)).equals((String) valueOdPK)) {
                                                                        //delete here
                                                                        flag = true ;
                                                                        currentBucketToDelete.Items.removeElementAt(b);
                                                                        if (currentBucketToDelete.Items.isEmpty()) {
                                                                            //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                            try {
                                                                                File f = new File(path1);           //file to be delete
                                                                                if (f.delete())                      //returns Boolean value
                                                                                {
                                                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                } else {
                                                                                    System.out.println("failed");
                                                                                }
                                                                            } catch (Exception e1) {
                                                                                e1.printStackTrace();
                                                                            }
                                                                            ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                            if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                //removed the id
                                                                                String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                currentGridToDelete.bucketIDs.removeElement(id);
                                                                                currentGridToDelete.ser(currentGrid);
                                                                                break;
                                                                            } else {
                                                                                if (c > 0) {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                    bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                    bucket5.ser(path10);
                                                                                } else {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.overflow = false;
                                                                                    bucket5.idOfOverflow = null;
                                                                                    bucket5.ser(path10);
                                                                                }

                                                                            }
                                                                            currentGridToDelete.ser(currentGrid);

                                                                        } else {
                                                                            currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                            b--;
                                                                        }
                                                                        break;
                                                                    }
                                                                } else if (PKType.equals("java.util.Date")) {
                                                                    if (((Date) (currentBucketToDelete.Items.get(b)).get(0)).equals((Date) valueOdPK)) {
                                                                        //delete here
                                                                        flag = true ;
                                                                        currentBucketToDelete.Items.removeElementAt(b);
                                                                        if (currentBucketToDelete.Items.isEmpty()) {
                                                                            //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                            try {
                                                                                File f = new File(path1);           //file to be delete
                                                                                if (f.delete())                      //returns Boolean value
                                                                                {
                                                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                } else {
                                                                                    System.out.println("failed");
                                                                                }
                                                                            } catch (Exception e1) {
                                                                                e1.printStackTrace();
                                                                            }
                                                                            ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                            if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                //removed the id
                                                                                String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                currentGridToDelete.bucketIDs.removeElement(id);
                                                                                currentGridToDelete.ser(currentGrid);
                                                                                break;
                                                                            } else {
                                                                                if (c > 0) {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                    bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                    bucket5.ser(path10);
                                                                                } else {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.overflow = false;
                                                                                    bucket5.idOfOverflow = null;
                                                                                    bucket5.ser(path10);
                                                                                }

                                                                            }
                                                                            currentGridToDelete.ser(currentGrid);

                                                                        } else {
                                                                            currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                            b--;
                                                                        }
                                                                        break;
                                                                    }
                                                                } else if (PKType.equals("java.lang.Integer")) {
                                                                    if (((Integer) (currentBucketToDelete.Items.get(b)).get(0)).equals((Integer) valueOdPK)) {
                                                                        //delete here
                                                                        flag = true ;
                                                                        currentBucketToDelete.Items.removeElementAt(b);
                                                                        if (currentBucketToDelete.Items.isEmpty()) {
                                                                            //removethe bucket
//                                                                    ((String)((Vector)(grid.pathOfBuckets.get(o)).get(1)).get(t))
                                                                            try {
                                                                                File f = new File(path1);           //file to be delete
                                                                                if (f.delete())                      //returns Boolean value
                                                                                {
                                                                                    System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                                                } else {
                                                                                    System.out.println("failed");
                                                                                }
                                                                            } catch (Exception e1) {
                                                                                e1.printStackTrace();
                                                                            }
                                                                            ((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).removeElementAt(c);
                                                                            if (((Vector) (currentGridToDelete.pathOfBuckets.get(n).get(1))).isEmpty()) {
                                                                                //removed the id
                                                                                String id = (String) currentGridToDelete.pathOfBuckets.get(n).get(0);
                                                                                currentGridToDelete.pathOfBuckets.removeElementAt(n);
                                                                                currentGridToDelete.bucketIDs.removeElement(id);
                                                                                currentGridToDelete.ser(currentGrid);
                                                                                break;
                                                                            } else {
                                                                                if (c > 0) {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c - 1));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.hasOverflow = currentBucketToDelete.hasOverflow;
                                                                                    bucket5.pathOfOverflows = currentBucketToDelete.pathOfOverflows;
                                                                                    bucket5.ser(path10);
                                                                                } else {
                                                                                    String path10 = ((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c ));
                                                                                    Bucket bucket5 = null;
                                                                                    try {
                                                                                        // Reading the object from a file
                                                                                        FileInputStream file = new FileInputStream(path10);
                                                                                        ObjectInputStream in = new ObjectInputStream(file);
                                                                                        // Method for deserialization of object
                                                                                        bucket5 = (Bucket) in.readObject();
                                                                                        in.close();
                                                                                        file.close();
                                                                                    } catch (IOException ex) {
                                                                                        System.out.println("IOException is caught");
                                                                                    } catch (ClassNotFoundException ex) {
                                                                                        System.out.println("ClassNotFoundException is caught");
                                                                                    }
                                                                                    bucket5.overflow = false;
                                                                                    bucket5.idOfOverflow = null;
                                                                                    bucket5.ser(path10);
                                                                                }

                                                                            }
                                                                            currentGridToDelete.ser(currentGrid);

                                                                        } else {
                                                                            currentBucketToDelete.ser(((String) ((Vector) (currentGridToDelete.pathOfBuckets.get(n)).get(1)).get(c)));
                                                                            b--;
                                                                        }
                                                                        break;
                                                                    }
                                                                }


                                                            }
                                                            if(flag)
                                                                break;

                                                        }


                                                    }


                                                }


                                            }
//                                    System.out.println(valueOdPK);//now I have the value of PK of this raw
                                            page.rows.remove(j);
                                            ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).removeElement(valueOdPK);
                                            System.out.println(((Vector) database.tables.get(indexFound).pages.get(i).get(5)));//remove from the list of keys
                                            database.tables.get(indexFound).ids.removeElement(valueOdPK);
                                        } else {
                                            yes = true;
                                        }

                                        //handle deletion of pages
                                        if (PKType.equals("java.lang.String")) {
                                            if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                            {
                                                //handle delete of pages
                                                if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                {


                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                        //All its overflow pages should have the new min and max values
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                        String newmin = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                        String newmax = (String) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                        int counter = i + 1;
                                                        while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                            database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                            database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                            counter++;
                                                        }

                                                    } else { //case if it is overflow and has overflow !=-1 & true
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                    }
                                                }
                                                //if I have no overflows
                                                else {
                                                    //and I am an overflow to other page
                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                        database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                    }
                                                }

                                                try {
                                                    File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                    if (f.delete())                      //returns Boolean value
                                                    {
                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                    } else {
                                                        System.out.println("failed");
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }

                                                database.tables.get(indexFound).pages.remove(i);
                                                i--;
                                            } else {//not empty page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                    database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                    //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            String newmin2 = (String) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            String newmax2 = (String) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                    //do not forget the rest of cases
                                                }
                                                //serialize page
                                                try {
                                                    FileOutputStream fileOut =
                                                            new FileOutputStream(path);
                                                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                    out.writeObject(page);
                                                    out.close();
                                                    fileOut.close();

                                                } catch (IOException i2) {
                                                    i2.printStackTrace();
                                                }
                                            }

                                            //serialize db
                                            //serilize the database

                                            try {
                                                //Saving of object in a file
                                                String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                FileOutputStream file = new FileOutputStream(pathdb);
                                                ObjectOutputStream out = new ObjectOutputStream(file);

                                                // Method for serialization of object
                                                out.writeObject(database);

                                                out.close();
                                                file.close();

                                                System.out.println("Object has been serialized");

                                            } catch (IOException ex) {
                                                System.out.println("IOException is caught");
                                            }


                                        } else if (PKType.equals("java.lang.Integer")) {
                                            if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                            {
                                                //handle delete of pages
                                                if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                {


                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                        //All its overflow pages should have the new min and max values
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                        Integer newmin = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                        Integer newmax = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                        int counter = i + 1;
                                                        while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                            database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                            database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                            counter++;
                                                        }

                                                    } else { //case if it is overflow and has overflow !=-1 & true
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                    }
                                                }
                                                //if I have no overflows
                                                else {
                                                    //and I am an overflow to other page
                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                        database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                    }
                                                }

                                                try {

                                                    File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                    if (f.delete())                      //returns Boolean value
                                                    {
                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                    } else {
                                                        System.out.println("failed");
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }

                                                database.tables.get(indexFound).pages.remove(i);
                                                i--;
                                            } else {//not empty page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                    database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                    //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Integer newmin2 = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Integer newmax2 = (Integer) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                    //do not forget the rest of cases
                                                }
                                                //serialize page
                                                try {
                                                    FileOutputStream fileOut =
                                                            new FileOutputStream(path);
                                                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                    out.writeObject(page);
                                                    out.close();
                                                    fileOut.close();

                                                } catch (IOException i2) {
                                                    i2.printStackTrace();
                                                }
                                            }

                                            //serialize db
                                            //serilize the database

                                            try {
                                                //Saving of object in a file
                                                String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                FileOutputStream file = new FileOutputStream(pathdb);
                                                ObjectOutputStream out = new ObjectOutputStream(file);

                                                // Method for serialization of object
                                                out.writeObject(database);

                                                out.close();
                                                file.close();

                                                System.out.println("Object has been serialized");

                                            } catch (IOException ex) {
                                                System.out.println("IOException is caught");
                                            }


                                        } else if (PKType.equals("java.lang.Double")) {
                                            if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                            {
                                                //handle delete of pages
                                                if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                {


                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                        //All its overflow pages should have the new min and max values
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                        Double newmin = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                        Double newmax = (Double) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                        int counter = i + 1;
                                                        while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                            database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                            database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                            counter++;
                                                        }

                                                    } else { //case if it is overflow and has overflow !=-1 & true
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                    }
                                                }
                                                //if I have no overflows
                                                else {
                                                    //and I am an overflow to other page
                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                        database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                    }
                                                }

                                                try {
                                                    File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                    if (f.delete())                      //returns Boolean value
                                                    {
                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                    } else {
                                                        System.out.println("failed");
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }

                                                database.tables.get(indexFound).pages.remove(i);
                                                i--;
                                            } else {//not empty page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                    database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                    //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Double newmin2 = (Double) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Double newmax2 = (Double) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                    //do not forget the rest of cases
                                                }
                                                try {
                                                    FileOutputStream fileOut =
                                                            new FileOutputStream(path);
                                                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                    out.writeObject(page);
                                                    out.close();
                                                    fileOut.close();

                                                } catch (IOException i2) {
                                                    i2.printStackTrace();
                                                }
                                            }
                                            //serialize page

                                            //serialize db
                                            //serilize the database

                                            try {
                                                //Saving of object in a file
                                                String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                FileOutputStream file = new FileOutputStream(pathdb);
                                                ObjectOutputStream out = new ObjectOutputStream(file);

                                                // Method for serialization of object
                                                out.writeObject(database);

                                                out.close();
                                                file.close();

                                                System.out.println("Object has been serialized");

                                            } catch (IOException ex) {
                                                System.out.println("IOException is caught");
                                            }


                                        } else if (PKType.equals("java.lang.Date")) {
                                            if (((Vector) database.tables.get(indexFound).pages.get(i).get(5)).isEmpty())//if the page become empty 1-delete it 2-next page take kol 7agatha
                                            {
                                                //handle delete of pages
                                                if (((Boolean) database.tables.get(indexFound).pages.get(i).get(7))) //if it has overflow
                                                {


                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) {  //if it is original and has overflows
                                                        //All its overflow pages should have the new min and max values
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, -1);//Now the first overflow is the original
                                                        Date newmin = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).firstElement();
                                                        Date newmax = (Date) ((Vector) database.tables.get(indexFound).pages.get(i + 1).get(5)).lastElement();
                                                        int counter = i + 1;
                                                        while ((Boolean) database.tables.get(indexFound).pages.get(counter).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) { //index out of bounds??

                                                            database.tables.get(indexFound).pages.get(i + 1).put(3, newmin);
                                                            database.tables.get(indexFound).pages.get(i + 1).put(4, newmax);
                                                            counter++;
                                                        }

                                                    } else { //case if it is overflow and has overflow !=-1 & true
                                                        database.tables.get(indexFound).pages.get(i + 1).put(6, ((Integer) database.tables.get(indexFound).pages.get(i).get(6)));//not orginal
                                                    }
                                                }
                                                //if I have no overflows
                                                else {
                                                    //and I am an overflow to other page
                                                    if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) != -1) {
                                                        database.tables.get(indexFound).pages.get(i - 1).put(7, false);

                                                    }
                                                }

                                                try {
                                                    File f = new File((String) database.tables.get(indexFound).pages.get(i).get(0));           //file to be delete
                                                    if (f.delete())                      //returns Boolean value
                                                    {
                                                        System.out.println(f.getName() + " deleted");   //getting and printing the file name
                                                    } else {
                                                        System.out.println("failed");
                                                    }
                                                } catch (Exception e) {
                                                    e.printStackTrace();
                                                }

                                                database.tables.get(indexFound).pages.remove(i);
                                                i--;
                                            } else {//not empty page
                                                if ((Integer) database.tables.get(indexFound).pages.get(i).get(6) == -1) { //if orginal page kda kda ha update min max
//                                            database.tables.get(indexFound).pages.get(i).put(3, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement());
//                                            database.tables.get(indexFound).pages.get(i).put(4, ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement());
                                                    database.tables.get(indexFound).pages.get(i).put(2, (Integer) database.tables.get(indexFound).pages.get(i).get(2) - 1);//update the number of rows
//                                                ((Vector<?>) database.tables.get(indexFound).pages.get(i).get(5)).remove(j);//remove the key from the list of ids
                                                    //update the min and max of the curr page and all its overflows if I am original

//                                            int counter2 = i;
//                                            Date newmin2 = (Date) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).firstElement();
//                                            Date newmax2 = (Date) ((Vector) database.tables.get(indexFound).pages.get(i).get(5)).lastElement();
//                                            while ((Boolean) database.tables.get(indexFound).pages.get(counter2).get(7) && !(database.tables.get(indexFound).pages.isEmpty())) {
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(3, newmin2);
//                                                database.tables.get(indexFound).pages.get(counter2 + 1).put(4, newmax2);
//                                                counter2++;
//                                            }

                                                    //do not forget the rest of cases
                                                }
                                                //serialize page
                                                try {
                                                    FileOutputStream fileOut =
                                                            new FileOutputStream(path);
                                                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
                                                    out.writeObject(page);
                                                    out.close();
                                                    fileOut.close();

                                                } catch (IOException i2) {
                                                    i2.printStackTrace();
                                                }
                                            }

                                            //serialize db
                                            //serilize the database

                                            try {
                                                //Saving of object in a file
                                                String pathdb = "src/main/resources/data/" + dataBaseName + ".ser";
                                                FileOutputStream file = new FileOutputStream(pathdb);
                                                ObjectOutputStream out = new ObjectOutputStream(file);

                                                // Method for serialization of object
                                                out.writeObject(database);

                                                out.close();
                                                file.close();

                                                System.out.println("Object has been serialized");

                                            } catch (IOException ex) {
                                                System.out.println("IOException is caught");
                                            }


                                        }

                                    }

                                }


                            }


                        }


                    }


                }


            }


        } else {
            throw new DBAppException();
        }


    }
    public static HashMap<Integer, Object> theArrNeeded(Hashtable<String, Object> colName, ArrayList<String[]> dataTypes) {
        HashMap<Integer, Object> result = new HashMap<>();
        Enumeration<String> a = colName.keys();
        while (a.hasMoreElements()) {
            String key = a.nextElement();
            for (int i = 0; i < dataTypes.size(); i++) {
                ArrayList<Object> element = new ArrayList<>();
                if (dataTypes.get(i)[1].equals(key)) {
                    element.add(dataTypes.get(i)[2]);
                    element.add(colName.get(key));
                    result.put(i, element);
                }
            }
        }
        return result;

    }

    public static ArrayList<Object> pkUsed(Hashtable<String, Object> colName, ArrayList<String[]> dataTypes) {
        Enumeration<String> a = colName.keys();
        System.out.println(colName.toString());
        System.out.println(dataTypes.toString());
        ArrayList<Object> result = null;
        while (a.hasMoreElements()) {
            String key = a.nextElement();
            for (int i = 0; i < dataTypes.size(); i++) {
                if (key.equals(dataTypes.get(i)[1])) {
                    if (dataTypes.get(i)[3].equals("True")) {
                        result = new ArrayList<>();
                        result.add(i);
                        result.add(dataTypes.get(i)[2]);
                        result.add(colName.get(key));
                        return result;
                    }
                }
            }

        }
        return result;

    }

    public String arrayTOString(ArrayList s) {
        String a = "[ ";
        for (int i = 0; i < s.size(); i++) {
            a = a + s.get(i) + " , ";

        }
        a = a + " ]";

        return a;
    }

    public static String PrimaryKey(String tableName) {
        String y = "";
        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        try {
//            br.readLine(); // read one line at a time, this reads the first line that has the tags.
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        try {
            while (br.ready()) { // as long as the buffer(file) is not empty read
                String x = null;
                try {
                    x = br.readLine(); // read the line and save it in x
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(","); // make the split beacuse the line cont things other than the table name

                for (int i = 0; 7 * i < arr.length; i++) {


                    if (arr[7 * i].equals(tableName) && arr[7 * i + 3].equals("True")) {
                        y = arr[7 * i + 1];


                    }
                }

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return y;

    }

    public static ArrayList ColoumnsInAtable(String tableName) {
        ArrayList<String> coloumnNames = new ArrayList();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        try {
//            br.readLine(); // read one line at a time, this reads the first line that has the tags.
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        try {
            while (br.ready()) { // as long as the buffer(file) is not empty read
                String x = null;
                try {
                    x = br.readLine(); // read the line and save it in x
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(","); // make the split beacuse the line cont things other than the table name

                //for(int i = 0;7*i<arr.length;i++){


                if (arr[0].equals(tableName)) {

                    coloumnNames.add(arr[1]);

                }
                //}

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return coloumnNames;

    }

    public static ArrayList DatatypesInTable(String tableName) {
        ArrayList<String> Datatypes = new ArrayList();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        try {
//            br.readLine(); // read one line at a time, this reads the first line that has the tags.
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        try {
            while (br.ready()) { // as long as the buffer(file) is not empty read
                String x = null;
                try {
                    x = br.readLine(); // read the line and save it in x
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(","); // make the split beacuse the line cont things other than the table name

                // for(int i = 0;7*i+1<arr.length;i++){


                if (arr[0].equals(tableName)) {
                    Datatypes.add(arr[2]);


                }
                // }

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return Datatypes;

    }

    public int getIndexKey(String tableName, String Key) {
        ArrayList Col = ColoumnsInAtable(tableName);
        // System.out.println("required key name "+Key);


        for (int i = 0; i < Col.size(); i++) {
//            System.out.println("loop number "+i);
//            System.out.println("current coloumn name "+Col.get(i));


            if (((String) (Col.get(i))).equals(Key)) {
                //   System.out.println("yesssss");
                return i;
            }
        }
        return -1;
    }


    public static boolean isPrimaryKey(String table, String coloumn) {
        if (PrimaryKey(table).equals(coloumn))
            return true;

        else
            return false;
    }

    public boolean hasIndex(Vector<String> coloumns, String table_name) {
        Table t = FindTable(table_name);
//
//        for (int i = 0; i < t.indicies.size(); i++) {
//            boolean flag = true;
//
//            for (int j = 0; j < coloumns.size(); j++) {
//
//                Vector<String> Index = (Vector) t.indicies.get(i).get(1);
//                Object x = coloumns.get(j);
//                if (!Index.contains(x)) {
//                    flag = false;
//                }
//                if (flag == false) {
//                    break;
//                } else if (j == coloumns.size() - 1) {
//                    return true;
//                }
//            }
//        }
//
//        //    for(int w=0;w<coloumns.size();)
//        String FirstColName = coloumns.get(0);
//        for (int g = 0; g < t.indicies.size(); g++) {
//            if (t.indicies.size() == 1 && t.indicies.contains(FirstColName))
//                return true;
//        }
//        return false;
        for (int i = 0; i < t.indicies.size(); i++) {
            for (int j = 0; j < coloumns.size(); j++) {
                Vector<String> Index = (Vector) t.indicies.get(i).get(1);
                Object x = coloumns.get(j);
                if (!Index.contains(x)) {
                    break;
                }

                if (j == coloumns.size() - 1 && ((Vector) t.indicies.get(i).get(1)).size() == coloumns.size()) {
                    System.out.println("fe awl print");
                    return true;
                }
            }

            // return
        }
        String FirstColName = coloumns.get(0);
        for (int k = 0; k < coloumns.size(); k++) {
            for (int g = 0; g < t.indicies.size(); g++) {
                if (((Vector) t.indicies.get(g).get(1)).size() == 1 && t.indicies.contains(coloumns.get(k))) {
                    System.out.println("fe tany return");
                    return true;
                }
            }
        }
        return false;


    }


    public String getPathGD(Vector<String> coloumns, String table_name) {
        Table t = FindTable(table_name);
        for (int i = 0; i < t.indicies.size(); i++) {
            for (int j = 0; j < coloumns.size(); j++) {
                Vector<String> Index = (Vector) t.indicies.get(i).get(1);
                Object x = coloumns.get(j);
                if (!Index.contains(x)) {
                    break;
                }

                if (j == coloumns.size() - 1 && ((Vector) t.indicies.get(i).get(1)).size() == coloumns.size()) {
                    System.out.println("fe awl print");
                    return (String) t.indicies.get(i).get(0);
                }
            }

            // return
        }
        String FirstColName = coloumns.get(0);
        for (int k = 0; k < coloumns.size(); k++) {
            for (int g = 0; g < t.indicies.size(); g++) {
                if (((Vector) t.indicies.get(g).get(1)).size() == 1 && t.indicies.contains(coloumns.get(k))) {
                    System.out.println("fe tany return");
                    return (String) t.indicies.get(g).get(0);
                }
            }
        }


        return null;
    }

    public static String dataTypeOfCol(String table_name, String col_name) {
        ArrayList<String> DataTypes = DatatypesInTable(table_name);
        ArrayList<String> Coloumns = ColoumnsInAtable(table_name);
        for (int i = 0; i < Coloumns.size(); i++) {
            if (Coloumns.get(i).equals(col_name))
                return DataTypes.get(i);
        }
        return null;
    }

    public static Vector<String> arrangeCol(Vector<String> colNames, Vector<String> columnNames, Vector<String> colValues) {
        Vector<String> arrangedValues = new Vector<>();
        for (int u = 0; u < colNames.size(); u++) {
            int indexOFCurrentCol;
            for (int p = 0; p < columnNames.size(); p++) {
                if (colNames.get(u).equals((columnNames.get(p)))) {

                    arrangedValues.add(colValues.get(p));
                    break;
                }

            }
        }
        return arrangedValues;
    }

    public static Vector<String> SearchByIndex(GridIndex GD, Vector<String> columnNames, Vector<String> colValues, Vector<String> operators, String table_name) throws DBAppException, ParseException {
        System.out.println("d5lt el search by index");
        System.out.println(colValues);
        System.out.println(columnNames);

        String CurrentCol = new String();
        String requiredDataType = new String();
        ArrayList<String> finalBucketIndex = new ArrayList<>();
        boolean flag = true;

        Vector<String> values = arrangeCol(GD.colNames, columnNames, colValues);

        Vector<String> operatorFinal = arrangeCol(GD.colNames, columnNames, operators);
        System.out.println(colValues);
        System.out.println(GD.colNames);

        for (int i = 0; i < GD.colNames.size(); i++) {//loop on all coloumns

            requiredDataType = dataTypeOfCol(table_name, GD.colNames.get(i));

            String currentColValue = values.get(i);


            String currentOperator = operatorFinal.get(i);
            int numberOfRanges = GD.Dimensions.get(i).arr.size();
            ArrayList<String> bucketIndex = new ArrayList<>();
            for (int j = 0; j < GD.Dimensions.get(i).arr.size(); j++) { //loop all ranges
                String[] currentRange = GD.Dimensions.get(i).arr.get(j);
                String min = currentRange[0];
                String max = currentRange[1];
                System.out.println("min +" + min);
                System.out.println("max : " + max);
                System.out.println(requiredDataType);
                String jString = j + "";
                if (requiredDataType.contains("String")) {
                    //if the operator is equal i am going to add only one page whih contains it's range
                    if (currentOperator.equals("=")) {
                        if ((max).compareTo((String) (currentColValue)) >= 0) {   //if the value is between the minmuim and maxmuim

                            bucketIndex.add(jString);
                            break;
                        }
                        // if the operator is not equal then i must searcbh in all pages
                        if (currentOperator.equals("!=")) {
                            for (int u = 0; u < numberOfRanges; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;
                        }
                        //if it has any other operator in the type string it will throw a db app exception as it violates syntax
                        else if (currentOperator.contains(">") || currentOperator.contains("<")) {
                            throw new DBAppException();
                        }

                    }
                }
                if (requiredDataType.contains("int") || requiredDataType.contains("Int")) {

                    System.out.println("da5alt fe el ineteger coondition ");
                    Integer x = Integer.parseInt(currentColValue);
                    Integer max1 = Integer.parseInt(max);
                    Integer min1 = Integer.parseInt(min);
                    if (currentOperator.equals("=")) {
                        if (x <= max1) {   //if the value is between the minmuim and maxmuim
                            bucketIndex.add(jString);

                            break;
                        }
                    } else if (currentOperator.equals("!=")) {
                        for (int u = 0; u < numberOfRanges; u++) {
                            bucketIndex.add(u + "");
                        }
                        break;
                    } else if (currentOperator.equals(">") || (currentOperator.equals(">="))) {
                        if (min1 >= x || max1 >= x) {
                            for (int u = j; u < numberOfRanges; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;

                        }

                    } else if (currentOperator.equals("<") || (currentOperator.equals("<="))) {
                        if (min1 <= x) {
                            for (int u = 0; u < j; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;
                        }
                    }


                }
                if (requiredDataType.contains("float") || requiredDataType.contains("Float") || requiredDataType.contains("Double")) {
                    System.out.println("da5alt fe el float coondition ");
                    System.out.println("Current col value : " + currentColValue);
                    Float x = Float.parseFloat((String) currentColValue);
                    Float max1 = Float.parseFloat(max);
                    Float min1 = Float.parseFloat(min);

                    if (currentOperator.equals("=")) {
                        if (x <= max1) {   //if the value is between the minmuim and maxmuim
                            bucketIndex.add(jString);

                            break;
                        }
                    } else if (currentOperator.equals("!=")) {
                        for (int u = 0; u < numberOfRanges; u++) {
                            bucketIndex.add(jString);
                        }
                        break;
                    } else if (currentOperator.equals(">") || (currentOperator.equals(">="))) {
                        if (min1 >= x || max1 >= x) {
                            for (int u = j; u < numberOfRanges; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;

                        }

                    } else if (currentOperator.equals("<") || (currentOperator.equals("<="))) {
                        if (min1 <= x) {
                            for (int u = 0; u < j; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;
                        }
                    }
                }
                if (requiredDataType.contains("Date") || requiredDataType.contains("date")) {
                    SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                    Date maxDate = sdformat.parse((String) max);
                    Date minDate = sdformat.parse((String) min);
//                    Date PKDate = sdformat.parse((String)clusteringKeyValue);
                    Date x = parseDate((String) currentColValue);
//                    Date min1 = (Date) min;
//                    Date max1 = (Date) max;

                    if (currentOperator.equals("=")) {
                        //x <= max1
                        if (maxDate.compareTo(x) >= 0) {   //if the value is between the minmuim and maxmuim
                            bucketIndex.add(jString);

                            break;
                        }
                    } else if (currentOperator.equals("!=")) {
                        for (int u = 0; u < numberOfRanges; u++) {
                            bucketIndex.add(u + "");
                        }
                        break;
                    } else if (currentOperator.equals(">") || (currentOperator.equals(">="))) {
                        //min1>=x||max1>=x)
                        if (minDate.compareTo(x) >= 0 || maxDate.compareTo(x) >= 0) {
                            for (int u = j; u < numberOfRanges; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;

                        }

                    } else if (currentOperator.equals("<") || (currentOperator.equals("<="))) {
                        //min1<=x
                        if (x.compareTo(minDate) >= 0) {
                            for (int u = 0; u < j; u++) {
                                bucketIndex.add(u + "");
                            }
                            break;
                        }
                    }
                }


            }
            System.out.println("bucket index " + bucketIndex);

            ArrayList<String> temp = new ArrayList<>();
            if (flag) {
                flag = false;
                finalBucketIndex = bucketIndex;
            } else {
                for (int h = 0; h < finalBucketIndex.size(); h++) {
                    for (int k = 0; k < bucketIndex.size(); k++) {
                        temp.add(finalBucketIndex.get(h) + bucketIndex.get(k));
                    }
                }

                finalBucketIndex = temp;
            }
            System.out.println("final bycket index " + finalBucketIndex);

        }
        ArrayList<String> temp2 = new ArrayList<>();
        for (int y = 0; y < finalBucketIndex.size(); y++) {
            if (GD.bucketIDs.contains(finalBucketIndex.get(y))) {
                temp2.add(finalBucketIndex.get(y));
            }
        }
        finalBucketIndex = temp2;
        Vector<String> pagesPath = new Vector<>();
        for (int r = 0; r < finalBucketIndex.size(); r++) {    //loop through the buckets i have to get the path of each bucket
            Vector<String> pathOfBucket = getPathsOfBucket(GD, finalBucketIndex.get(r));//all paths of a bucket and it's overflow

            System.out.println("path of bucket " + pathOfBucket);
            for (int d = 0; d < pathOfBucket.size(); d++) {//loop through each indvidual bucket to get the paths of pages each bucket contains on its own
                Bucket bk = null;
                try {
                    // Reading the object from a file
                    FileInputStream file = new FileInputStream(pathOfBucket.get(d));
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
                for (int q = 0; q < bk.Items.size(); q++) {
                    pagesPath.add((String) bk.Items.get(q).get(1));
                }
                bk.ser(pathOfBucket.get(d));

            }
        }
        System.out.println("5alst el search by index");

        return pagesPath;
    }

    public static Vector<String> getPathsOfBucket(GridIndex GD, String id) {
        for (int i = 0; i < GD.pathOfBuckets.size(); i++) {
            if (GD.pathOfBuckets.get(i).get(0).equals(id))
                return (Vector) GD.pathOfBuckets.get(i).get(1);
        }
        return null;
    }

    public ArrayList<Integer> PathIntoPageIndex(Vector<String> PagePath, String Table_Name) {
        System.out.println(Table_Name);
        System.out.println(dataBaseName);
        Table table = FindTable(Table_Name);
        ArrayList<Integer> PageIndex = new ArrayList<>();
        for (int i = 0; i < PagePath.size(); i++) {
            for (int j = 0; j < table.pages.size(); j++) {
                if (table.pages.get(j).get(0).equals(PagePath.get(i))) {
                    if (!PageIndex.contains(j))
                        PageIndex.add(j);
                    break;
                }
            }
        }
        return PageIndex;
    }

    @Override
    public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException, ParseException {
        //  System.out.println("da5lt fe el select");
        boolean tableExist = false;
        Iterator itr = null;
        ArrayList<String> tables = namOfTables();
        Vector<Vector> oldValue = new Vector<>();
        Vector<Vector> Results = new Vector<>();
        int indexOfOperator = 0;
        Vector<String> indexNeeded = new Vector<>();
        int numberOfOperators = arrayOperators.length;
        Vector<Vector> FinalRows = new Vector<>();
        int coloumnIndexGD = 0;
        for (int r = 0; r < sqlTerms.length; r++) {
            if (!(indexNeeded.contains(sqlTerms[r]._strColumnName)))
                indexNeeded.add(sqlTerms[r]._strColumnName);
        }
        System.out.println(indexNeeded);
        boolean flagIndex = false;
        String GDpath = new String();
        System.out.println("HAS INDEX VALUE " + hasIndex(indexNeeded, sqlTerms[0]._strTableName));
        if (hasIndex(indexNeeded, sqlTerms[0]._strTableName)) {
            flagIndex = true;
            GDpath = getPathGD(indexNeeded, sqlTerms[0]._strTableName);
        }

        for (int i = 0; i < sqlTerms.length; i++) {
//            System.out.println("Sql terms length : "+ sqlTerms.length);
//            System.out.println("i : "+i);
            oldValue = FinalRows;
            FinalRows = new Vector<>();
            boolean index_found = false;
            boolean isPrimary = false;
            String tableName = sqlTerms[i]._strTableName;
            String ColoumnName = sqlTerms[i]._strColumnName;
            Object ColoumnValue = sqlTerms[i]._objValue;
            String operator = sqlTerms[i]._strOperator;
            ArrayList Coloumns = ColoumnsInAtable(tableName);
            String pk = PrimaryKey(tableName);
            Table table = FindTable(tableName);
            isPrimary = isPrimaryKey(tableName, ColoumnName);
            Vector<String> AllColoumns = new Vector<>();
            Vector<String> AllValues = new Vector<>();
            Vector<String> AllOperators = new Vector<>();

            for (int z = 0; z < sqlTerms.length; z++) {
                AllColoumns.add(sqlTerms[z]._strColumnName);
                AllOperators.add(sqlTerms[z]._strOperator);
                String tempValue = sqlTerms[z]._objValue + "";
                AllValues.add(tempValue);
            }

            for (int k = 0; k < tables.size(); k++) {
                if (tableName.equals(tables.get(k))) {
                    tableExist = true;


                }

            }
            //check if the table and coloumn exists
            if (tableExist == false)
                throw new DBAppException();


            if (!Coloumns.contains(ColoumnName)) {

                throw new DBAppException();
            }


//            if (!index_found) {
//                if (isPrimary) {
//                    Table table1=FindTable(tableName);
//                    for(int j =0;j<table1.pages.size();j++) {
//                        Vector<HashMap> page = table1.pages;
//                        HashMap page22=page.get(j);
//
//                    }
//                }
//
//            }

            // 0 file path --- 1 id ----- 2 numberofRows -----3 min ------4 max
            Page pag = null;
            ArrayList<Integer> PageIndex = new ArrayList<>();
            Table t = FindTable(tableName);
            int LastPage = t.pages.size();
            //System.out.println(t.pages.size());
            if (flagIndex) {
                GridIndex GD = null;
                try {
                    // Reading the object from a file
                    System.out.println(GDpath);
                    FileInputStream file = new FileInputStream(GDpath);

                    ObjectInputStream in = new ObjectInputStream(file);

                    // Method for deserialization of object
                    GD = (GridIndex) in.readObject();

                    in.close();
                    file.close();
                } catch (IOException ex) {
                    System.out.println("exception fe deserlizing the grid");
                    System.out.println("IOException is caught");
                } catch (ClassNotFoundException ex) {
                    System.out.println("ClassNotFoundException is caught");
                }
                Vector<String> temp = SearchByIndex(GD, AllColoumns, AllValues, AllOperators, tableName);
                System.out.println("pages paths " + temp);
                PageIndex = PathIntoPageIndex(temp, tableName);
                //serlizing the gridIndex once more
//                try {
//                    //Saving of object in a file
//                    FileOutputStream file = new FileOutputStream(GDpath);
//                    ObjectOutputStream out = new ObjectOutputStream(file);
//
//                    // Method for serialization of object
//                    out.writeObject(this);
//
//                    out.close();
//                    file.close();
//
//                    System.out.println("Object has been serialized");
//
//                } catch (IOException ex) {
//                    System.out.println("IOException is caught");
//                }

            } else if (isPrimary) {
                System.out.println(isPrimary);
                System.out.println(ColoumnValue);

                try {


                    int FirstPage = 0;


                    for (int y = 0; y < LastPage; y++) {
                        HashMap v = t.pages.get(y);
                        System.out.println("hashmap v :" + v);
                        String max = v.get(4) + "";
                        String min = v.get(3) + "";
                        int numOfRows = (int) v.get(2);
                        String path = (String) v.get(0);
                        String pk2 = PrimaryKey(tableName);
                        int indexOfPrimaryKey = getIndexKey(tableName, pk2);
                        String ColoumnValueString = (String) ColoumnValue;

                        ArrayList dataTypesIHave = DatatypesInTable(tableName);

                        String PageMaxdatatype = (String) dataTypesIHave.get(indexOfPrimaryKey);
                        //max.getClass().getSimpleName();
                        if (PageMaxdatatype.contains("String")) {
                            //if the operator is equal i am going to add only one page whih contains it's range
                            if (operator.equals("=")) {
                                if ((max).compareTo((String) (ColoumnValue)) >= 0) {   //if the value is between the minmuim and maxmuim

                                    PageIndex.add(y);
                                    break;
                                }
                                // if the operator is not equal then i must searcbh in all pages
                                if (operator.equals("!=")) {
                                    for (int u = 0; u < LastPage; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;
                                }
                                //if it has any other operator in the type string it will throw a db app exception as it violates syntax
                                else if (operator.contains(">") || operator.contains("<")) {
                                    throw new DBAppException();
                                }

                            }
                        }
                        if (PageMaxdatatype.contains("int") || PageMaxdatatype.contains("Int")) {

                            System.out.println("da5alt fe el ineteger coondition ");
                            Integer x = Integer.parseInt(ColoumnValueString);
                            Integer max1 = Integer.parseInt(max);
                            Integer min1 = Integer.parseInt(min);
                            if (operator.equals("=")) {
                                if (x <= max1) {   //if the value is between the minmuim and maxmuim
                                    PageIndex.add(y);

                                    break;
                                }
                            } else if (operator.equals("!=")) {
                                for (int u = 0; u < LastPage; u++) {
                                    PageIndex.add(u);
                                }
                                break;
                            } else if (operator.equals(">") || (operator.equals(">="))) {
                                if (min1 >= x || max1 >= x) {
                                    for (int u = y; u < LastPage; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;

                                }

                            } else if (operator.equals("<") || (operator.equals("<="))) {
                                if (min1 <= x) {
                                    for (int u = 0; u < y; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;
                                }
                            }


                        }
                        if (PageMaxdatatype.contains("float") || PageMaxdatatype.contains("Float") || PageMaxdatatype.contains("Double")) {
                            // System.out.println("da5alt fe el float coondition ");
                            Float x = Float.parseFloat(ColoumnValueString);
                            Float max1 = Float.parseFloat(max);
                            Float min1 = Float.parseFloat(min);

                            if (operator.equals("=")) {
                                if (x <= max1) {   //if the value is between the minmuim and maxmuim
                                    PageIndex.add(y);

                                    break;
                                }
                            } else if (operator.equals("!=")) {
                                for (int u = 0; u < LastPage; u++) {
                                    PageIndex.add(u);
                                }
                                break;
                            } else if (operator.equals(">") || (operator.equals(">="))) {
                                if (min1 >= x || max1 >= x) {
                                    for (int u = y; u < LastPage; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;

                                }

                            } else if (operator.equals("<") || (operator.equals("<="))) {
                                if (min1 <= x) {
                                    for (int u = 0; u < y; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;
                                }
                            }
                        }
                        if (PageMaxdatatype.contains("Date") || PageMaxdatatype.contains("date")) {
                            SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");

//                    Date maxDate = sdformat.parse((String)max);
//                    Date minDate = sdformat.parse((String)min);
//                    Date PKDate = sdformat.parse((String)clusteringKeyValue);
                            Date x = parseDate(ColoumnValueString);
                            Date min1 = (Date) v.get(3);
                            Date max1 = (Date) v.get(4);

                            if (operator.equals("=")) {
                                //x <= max1
                                if (max1.compareTo(x) >= 0) {   //if the value is between the minmuim and maxmuim
                                    PageIndex.add(y);

                                    break;
                                }
                            } else if (operator.equals("!=")) {
                                for (int u = 0; u < LastPage; u++) {
                                    PageIndex.add(u);
                                }
                                break;
                            } else if (operator.equals(">") || (operator.equals(">="))) {
                                //min1>=x||max1>=x)
                                if (min1.compareTo(x) >= 0 || max1.compareTo(x) >= 0) {
                                    for (int u = y; u < LastPage; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;

                                }

                            } else if (operator.equals("<") || (operator.equals("<="))) {
                                //min1<=x
                                if (x.compareTo(min1) >= 0) {
                                    for (int u = 0; u < y; u++) {
                                        PageIndex.add(u);
                                    }
                                    break;
                                }
                            }
                        }


                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            } else if (!(isPrimary)) {
                for (int h = 0; h < LastPage; h++) {
                    PageIndex.add(h);
                }
            }

            System.out.println("page index :" + PageIndex);
            //loop through all the page that i have added to my list
            for (int r = 0; r < PageIndex.size(); r++) {

                int current_PageIndex = PageIndex.get(r);
                System.out.println("Current Page Index : " + current_PageIndex);
                HashMap v = table.pages.get(current_PageIndex);
                System.out.println("hashmap v :" + v);

                //loop through each page to check all the rows

                Vector vectorOfPKs = (Vector) v.get(5);
                ArrayList listOfPKs = new ArrayList<>(vectorOfPKs);

                String path = (String) v.get(0);
                FileInputStream fileIn = null;
                try {
                    fileIn = new FileInputStream(path);

                    ObjectInputStream in = new ObjectInputStream(fileIn);
                    pag = (Page) in.readObject();
                    in.close();
                    fileIn.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
                int n = pag.rows.size();
                System.out.println("pag.rows.size : " + n);
                for (int u = 0; u < n; u++) {
                    // el hya el mafrod zay numOfRows bzbtpag.rows.size()
                    Vector row = (Vector) pag.rows.get(u);

                    int requiredColoumnIndex = getIndexKey(tableName, ColoumnName);
                    Object CurrentRowValue = row.get(requiredColoumnIndex);
                    ArrayList dataTypesIHave = DatatypesInTable(tableName);

                    String ColoumnDataType = (String) dataTypesIHave.get(requiredColoumnIndex);
                    //max.getClass().getSimpleName();
                    if (ColoumnDataType.contains("String")) {
                        //if the operator is equal i am going to add only one page whih contains it's range
                        if (operator.equals("=")) {
                            if ((CurrentRowValue).equals(ColoumnValue)) {
                                //if the value is between the minmuim and maxmuim
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);
                                System.out.println("Row " + row);

                            }
                        } else if (operator.contains("!=")) {
                            System.out.println("not equal in string ");
                            if ((!((CurrentRowValue).equals(ColoumnValue))) && (!FinalRows.contains(row))) {   //if the value is between the minmuim and maxmuim
                                FinalRows.add(row);

                            }
                        }
                        //if it has any other operator in the type string it will throw a db app exception as it violates syntax
                        else if (operator.contains(">") || operator.contains("<")) {
                            throw new DBAppException();


                        }
                    }
                    if (ColoumnDataType.contains("int") || ColoumnDataType.contains("Int")) {


                        Integer x = Integer.parseInt((String) ColoumnValue);
                        Integer rowValue = Integer.parseInt((String) CurrentRowValue);
                        if (operator.equals("=")) {
                            if (x == rowValue) {
                                if (!FinalRows.contains(row))//if the value is between the minmuim and maxmuim
                                    FinalRows.add(row);
                            }
                        } else if (operator.equals("!=")) {
                            if (!(x == rowValue)) {
                                //if the value is between the minmuim and maxmuim
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);
                            }
                        } else if (operator.equals("<")) {
                            if (x > rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }

                        } else if (operator.equals(("<="))) {
                            if (x >= rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals(">")) {
                            if (x < rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals(">=")) {
                            if (x <= rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        }


                    }
                    if (ColoumnDataType.contains("float") || ColoumnDataType.contains("Float") || ColoumnDataType.contains("Double")) {
                        // System.out.println("da5alt fe el float coondition ");
                        String ColoumnValueString2 = ColoumnValue + "";
                        Float x = Float.parseFloat(ColoumnValueString2);
                        String CurrentRowValueString = CurrentRowValue + "";
                        Float rowValue = Float.parseFloat((CurrentRowValueString));

                        if (operator.equals("=")) {


                            if (x.equals(rowValue)) {   //if the value is between the minmuim and maxmuim
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals("!=")) {
                            if (!(x == rowValue)) {   //if the value is between the minmuim and maxmuim
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);
                            }
                        } else if (operator.equals("<")) {
                            if (x > rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }

                        } else if (operator.equals(("<="))) {
                            if (x >= rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals(">")) {
                            if (x < rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals(">=")) {
                            if (x <= rowValue) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        }


                    }


                    if (ColoumnDataType.contains("Date") || ColoumnDataType.contains("date")) {
                        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd");
                        System.out.println("coloumn value in date " + ColoumnValue);
//                    Date maxDate = sdformat.parse((String)max);
//                    Date minDate = sdformat.parse((String)min);
//                    Date PKDate = sdformat.parse((String)clusteringKeyValue);
                        Date x = parseDate((String) ColoumnValue);
                        Date rowValue = (Date) CurrentRowValue;
//                        Date max1 = (Date) v.get(4);
                        // System.out.println("max "+v.get(4));
                        // Date max1 =sdformat.parse((String)v.get(4));


                        if (operator.equals("=")) {
                            if (x == rowValue) {   //if the value is between the minmuim and maxmuim
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);
                            }
                        } else if (operator.equals("!=")) {
                            if (!(x == rowValue)) {   //if the value is between the minmuim and maxmuim
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);
                            }
                        } else if (operator.equals(">")) {
                            if (x.compareTo(rowValue) < 0) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }

                        } else if (operator.equals((">="))) {
                            if (x.compareTo(rowValue) <= 0) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals("<")) {
                            if (x.compareTo(rowValue) > 0) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        } else if (operator.equals("<=")) {
                            if (x.compareTo(rowValue) >= 0) {
                                if (!FinalRows.contains(row))
                                    FinalRows.add(row);

                            }
                        }

                    }


                }
                try {
                    FileOutputStream fileOut =
                            new FileOutputStream(path);
                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
                    out.writeObject(pag);
                    out.close();
                    fileOut.close();

                } catch (IOException i2) {
                    i2.printStackTrace();
                }

            }

            Vector delimter = new Vector();
            // delimter.add("#");
            // FinalRows.add(delimter);
            // System.out.println(FinalRows.size());
//            for(int q=0;q<FinalRows.size();q++){
//                System.out.println(FinalRows.get(q));
//            }
            Results = new Vector<>();
            String CurrentOperator = new String();
            if (indexOfOperator < numberOfOperators && i >= 1) {

                CurrentOperator = arrayOperators[indexOfOperator];
                indexOfOperator++;

                if (CurrentOperator.equals("OR")) {
                    Results = oldValue;

//                for(int b=0;b<oldValue.size();b++){
//                    Results.add(oldValue.get(b));
//                }
                    for (int a = 0; a < FinalRows.size(); a++) {
                        if (!Results.contains(FinalRows.get(a))) {
                            Results.add(FinalRows.get(a));
                        }
                    }
                    FinalRows = Results;
                    System.out.println("old value in and " + oldValue);
                    System.out.println("Final Rows in and " + FinalRows);

                } else if (CurrentOperator.equals("AND")) {
                    if (indexOfOperator == 1) {

                    }
                    for (int a = 0; a < FinalRows.size(); a++) {

                        System.out.println("old value in and " + oldValue);
                        System.out.println("Final Rows in and " + FinalRows);
                        if (oldValue.contains(FinalRows.get(a))) {
                            Results.add(FinalRows.get(a));
                        }
                    }
                    FinalRows = Results;
                } else if (CurrentOperator.equals("XOR")) {
                    Results = XOR(oldValue, FinalRows, tableName);
                    FinalRows = Results;
                }


            }
        }
        itr = FinalRows.iterator();
//        Iterator itr = FinalRows.iterator();
        return itr;
    }

    public Table FindTable(String TableName) {


        String filename = "src/main/resources/data/" + dataBaseName + ".ser";
        Database database = null;


        try {
            // Reading the object from a file
            FileInputStream file = new FileInputStream(filename);
            ObjectInputStream in = new ObjectInputStream(file);

            // Method for deserialization of object
            database = (Database) in.readObject();


            System.out.println("Deserialized");

            in.close();
            file.close();
        } catch (IOException ex) {
            System.out.println("IOException is caught");
        } catch (ClassNotFoundException ex) {
            System.out.println("ClassNotFoundException is caught");
        }
        Table temp = null;
        for (int i = 0; i < database.tables.size(); i++) {

            Table t = database.tables.get(i);
            if (t.table_name.equals(TableName)) {
                temp = t;
                break;
            }
        }
//        try {
//            //Saving of object in a file
//           String path = "src/main/resources/data/" + dataBaseName + ".ser";
//            FileOutputStream file = new FileOutputStream(path);
//            ObjectOutputStream out = new ObjectOutputStream(file);
//
//            // Method for serialization of object
//            out.writeObject(this);
//
//            out.close();
//            file.close();
//
//            System.out.println("Object has been serialized");
//
//        } catch (IOException ex) {
//            System.out.println("IOException is caught");
//        }
        return temp;
    }

    public Vector<Vector> XOR(Vector<Vector> oldValue, Vector<Vector> finalRows, String table_name) throws DBAppException {
        Vector<Vector> Term1 = AND(oldValue, NOT(finalRows, table_name));
        Vector<Vector> Term2 = AND(finalRows, NOT(oldValue, table_name));
        return OR(Term1, Term2);
    }

    public Vector<Vector> AND(Vector<Vector> FinalRows, Vector<Vector> oldValue) {
        Vector<Vector> Results = new Vector<Vector>();
        for (int a = 0; a < FinalRows.size(); a++) {
            if (oldValue.contains(FinalRows.get(a))) {
                Results.add(FinalRows.get(a));
            }
        }
        FinalRows = Results;
        return FinalRows;
    }

    public Vector<Vector> OR(Vector<Vector> FinalRows, Vector<Vector> oldValue) {
        Vector<Vector> Results = new Vector<Vector>();
        Results = oldValue;
        for (int a = 0; a < FinalRows.size(); a++) {
            if (!Results.contains(FinalRows.get(a))) {
                Results.add(FinalRows.get(a));
            }
        }
        FinalRows = Results;
        return FinalRows;
    }

    public int getPKIndex(String tbName) {
        int i = -1;
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            while (br.ready()) { // as long as the buffer(file) is not empty read
                String x = null;
                try {
                    x = br.readLine(); // read the line and save it in x
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (arr[0].equals(tbName)) {
                    i++;
                    if (arr[3].equals("True")) {
                        break;
                    }
                }
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(i);
        return i;

    }

    public String PKdataType(String tbName) {
        String PKType = "";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));// !!!change
            // the
            // path
        } catch (FileNotFoundException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }


        try {
            while (br.ready()) { // as long as the buffer(file) is not empty read
                String x = null;
                try {
                    x = br.readLine(); // read the line and save it in x
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                String[] arr = x.split(",");
                if (arr[3].equals("True") && arr[0].equals(tbName)) {
                    PKType = arr[2];
                    break;
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return PKType;

    }

    public Vector<Vector> NOT(Vector<Vector> rows, String TableName) throws DBAppException {
        Table table = FindTable(TableName);
        Page pag = null;
        Vector<Vector> FinalRows = new Vector<>();

        ArrayList<Integer> pageIndex = new ArrayList<>();
        for (int l = 0; l < table.pages.size(); l++) {
            pageIndex.add(l);
        }
        for (int y = 0; y < pageIndex.size(); y++) {
            HashMap v = table.pages.get(y);

            //loop through each page to check all the rows

            Vector vectorOfPKs = (Vector) v.get(5);
            ArrayList listOfPKs = new ArrayList<>(vectorOfPKs);

            String path = (String) v.get(0);
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(path);

                ObjectInputStream in = new ObjectInputStream(fileIn);
                pag = (Page) in.readObject();
                in.close();
                fileIn.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            for (int i = 0; i < pag.rows.size(); i++) {
                if (!(rows.contains(pag.rows.get(i)))) {
                    FinalRows.add((Vector) pag.rows.get(i));
                }

            }
            try {
                FileOutputStream fileOut =
                        new FileOutputStream(path);
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(pag);
                out.close();
                fileOut.close();

            } catch (IOException i2) {
                i2.printStackTrace();
            }

        }
        return FinalRows;
    }

    public static int binarySearchString(Vector<String> arr, int l, int r, String x) {
        if (r > l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).equals(x))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (((String) arr.get(mid)).compareTo(x) > 0)
                return binarySearchString(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchString(arr, mid + 1, r, x);
        } else if (r == l) {
            return l;
        }

        // We reach here when element is not present
        // in array
        return r;
    }

    public static int binarySearchInteger(Vector<Integer> arr, int l, int r, Integer x) {
        if (r > l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).equals(x))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (((Integer) arr.get(mid)).compareTo(x) > 0)
                return binarySearchInteger(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchInteger(arr, mid + 1, r, x);
        } else if (r == l) {
            return l;
        }

        // We reach here when element is not present
        // in array
        return r;
    }

    public static int binarySearchDouble(Vector<Double> arr, int l, int r, Double x) {
        if (r > l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).equals(x))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (((Double) arr.get(mid)).compareTo(x) > 0)
                return binarySearchDouble(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchDouble(arr, mid + 1, r, x);
        } else if (r == l) {
            return l;
        }

        // We reach here when element is not present
        // in array
        return r;
    }

    public static int binarySearchDate(Vector<Date> arr, int l, int r, Date x) {
        if (r > l) {
            int mid = l + (r - l) / 2;

            // If the element is present at the
            // middle itself
            if (arr.get(mid).equals(x))
                return mid;

            // If element is smaller than mid, then
            // it can only be present in left subarray
            if (((Date) arr.get(mid)).compareTo(x) > 0)
                return binarySearchDate(arr, l, mid - 1, x);

            // Else the element can only be present
            // in right subarray
            return binarySearchDate(arr, mid + 1, r, x);
        } else if (r == l) {
            return l;
        }

        // We reach here when element is not present
        // in array
        return r;
    }
//     public static void change(Vector<Vector> pages , int index , Object  changed)
//     {
//         for (int i = 0 ; i < pages.size() ; i++)
//         {
//             if((Integer)pages.get(i).get(6) == index)
//             {
//                 pages.get(i).set(3,changed);
//             }
//         }
//     }

    public static void main(String[] args) throws FileNotFoundException, DBAppException, ParseException {
//        DBApp dbApp = new DBApp();
//        dbApp.init();
//
//
//        SQLTerm[] arrSQLTerms = new SQLTerm[2];
//        arrSQLTerms[0] = new SQLTerm();
//
//        arrSQLTerms[0]._strTableName = "students";
//        arrSQLTerms[0]._strColumnName = "first_name";
//        arrSQLTerms[0]._strOperator = "=";
//        arrSQLTerms[0]._objValue = "pzSMNq";
//
//        arrSQLTerms[1] = new SQLTerm();
//        arrSQLTerms[1]._strTableName = "students";
//        arrSQLTerms[1]._strColumnName = "last_name";
//        arrSQLTerms[1]._strOperator = "=";
//        arrSQLTerms[1]._objValue = "NfdxAL";
//
////        arrSQLTerms[2] = new SQLTerm();
////        arrSQLTerms[2]._strTableName = "students";
////        arrSQLTerms[2]._strColumnName = "first_name";
////        arrSQLTerms[2]._strOperator = "=";
////        arrSQLTerms[2]._objValue = "WlykNk";
//        String[] strarrOperators = new String[1];
//        strarrOperators[0] = "AND";
////        strarrOperators[1] = "AND";
//        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//        while (resultSet.hasNext()) {
//            System.out.println(resultSet.next());
//        }
//        DBApp dbApp = new DBApp();
//        dbApp.init();
//
//
//        SQLTerm[] arrSQLTerms = new SQLTerm[2];
//        arrSQLTerms[0] = new SQLTerm();
//
//        arrSQLTerms[0]._strTableName = "students";
//        arrSQLTerms[0]._strColumnName = "gpa";
//        arrSQLTerms[0]._strOperator = ">";
//        arrSQLTerms[0]._objValue = "1.0";
//
//        arrSQLTerms[1] = new SQLTerm();
//        arrSQLTerms[1]._strTableName = "students";
//        arrSQLTerms[1]._strColumnName = "id";
//        arrSQLTerms[1]._strOperator = "=";
//        arrSQLTerms[1]._objValue = "66-1766";
//
////        arrSQLTerms[2] = new SQLTerm();
////        arrSQLTerms[2]._strTableName = "students";
////        arrSQLTerms[2]._strColumnName= "first_name";
////        arrSQLTerms[2]._strOperator = "=";
////        arrSQLTerms[2]._objValue = "ISJAnb";
//        String[] strarrOperators = new String[1];
//        strarrOperators[0] = "AND";
//        // strarrOperators[1]="AND";
//        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//        while (resultSet.hasNext()) {
//            System.out.println(resultSet.next());
//        }
//    }
    }
}
//        SString strTableName = "Student";
////        DBApp dbApp = new DBApp( );
////        Hashtable htblColNameType = new Hashtable( );
////        Hashtable min = new Hashtable( );
////        Hashtable max = new Hashtable( );
////
////        htblColNameType.put("id", "java.lang.Integer");
////        htblColNameType.put("name", "java.lang.String");
////        htblColNameType.put("gpa", "java.lang.double");
////
////        min.put("id","1");
////        min.put("name","A");
////        min.put("gpa","0.7");
////
////        max.put("id","10");
////        max.put("name","ZZZZZZZZZ");
////        max.put("gpa","5");
////
////
////        try {
////            dbApp.createTable( strTableName, "id", htblColNameType,min,max);
////        } catch (DBAppException e) {
////            e.printStackTrace();
////        }
////
//        //    String a = "2000-08-19";
//        //    System.out.println(parseDate(a));
//        String filename1 = "src/main/resources/data/DB2" +  ".ser";
//        Database database1 = null;
//        try {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream(filename1);
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            database1 = (Database) in.readObject();
//
//            in.close();
//            file.close();
//            System.out.println("deserilized1");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
////
//        for (int i = 0; i < 10; i++){
////
//            String filename = "src/main/resources/data/buckettranscriptscourse_namedate_passed40.ser";
//            Bucket database = null;
//            try {
//                // Reading the object from a file
//                FileInputStream file = new FileInputStream(filename);
//                ObjectInputStream in = new ObjectInputStream(file);
//
//                // Method for deserialization of object
//                database = (Bucket) in.readObject();
//
//                in.close();
//                file.close();
//                System.out.println("deserilized1");
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
//            }
////            System.out.println( database.rows.toString());
//
////        for (int i = 0 ; i < database.Dimensions.size();i++)
//            System.out.println(database.Items.toString());
//            System.out.println(database.Items.size());
        //    System.out.println(database.rows.size());
//        System.out.println(database.pathOfBuckets.toString());
////            System.out.println(database1.tables.get(0).pages.get(i-1).toString());
////            System.out.println(database1.tables.get(0).pages.get(0).toString());
////            System.out.println(database1.tables.get(0).pages.get(1).toString());
//            System.out.println(database1.tables.get(1).ids.toString());
//            System.out.println(database1.tables.get(1).ids.size());
//
//        }
//        for (int i = 1; i < 4; i++){
////
//            String filename1 = "src/main/resources/data/bucketcoursescourse_idhours10.ser";
//            Bucket database1 = null;
//            try {
//                // Reading the object from a file
//                FileInputStream file = new FileInputStream(filename1);
//                ObjectInputStream in = new ObjectInputStream(file);
//
//                // Method for deserialization of object
//                database1 = (Bucket) in.readObject();
//
//                in.close();
//                file.close();
//                System.out.println("deserilized1");
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//            System.out.println( database1.Items.toString());
//        System.out.println( database1.Items.size());
//
////        for (int i = 0 ; i < database.Dimensions.size();i++)
//        System.out.println(database.rows.toString());
//            System.out.println(database.rows.size());
////        System.out.println(database.pathOfBuckets.toString());
//////            System.out.println(database1.tables.get(0).pages.get(i-1).toString());
//////            System.out.println(database1.tables.get(0).pages.get(0).toString());
//////            System.out.println(database1.tables.get(0).pages.get(1).toString());
////            System.out.println(database1.tables.get(1).ids.toString());
////            System.out.println(database1.tables.get(1).ids.size());
////
//        }
////        filename = "src/main/resources/data/pcs" + 2+ ".ser";
////        database = null;
////        try {
////            // Reading the object from a file
////            FileInputStream file = new FileInputStream(filename);
////            ObjectInputStream in = new ObjectInputStream(file);
////
////            // Method for deserialization of object
////            database = (Page) in.readObject();
////
////            in.close();
////            file.close();
////            System.out.println("deserilized1");
////
////        } catch (IOException e) {
////            e.printStackTrace();
////        } catch (ClassNotFoundException e) {
////            e.printStackTrace();
////        }
////        System.out.println( database.rows.toString());
////        System.out.println( database.rows.size());
////
////        DBApp db = new DBApp();
////        db.init();
//
////        Hashtable<String,Object> x = new Hashtable<>();
////        Hashtable<String,Object> y = new Hashtable<>();
//////        x.put("id","1");
//////        x.put("age",2);
////        y.put("name","Max");
//////
////        db.deleteFromTable("dogs",y);
////
////
//        String   filename = "src/main/resources/data/courses1.ser";
//      Page   database = null;
//        try {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream(filename);
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            database = (Page) in.readObject();
//
//            in.close();
//            file.close();
//            System.out.println("deserilized1");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.out.println( database.rows.toString());
//        System.out.println( database.rows.size());
//        String   filename1 = "src/main/resources/data/coursescourse_idhours.ser";
//        GridIndex   database1 = null;
//        try {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream(filename1);
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            database1 = (GridIndex) in.readObject();
//
//            in.close();
//            file.close();
//            System.out.println("deserilized1");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.out.println( database1.pathOfBuckets.toString());
//        System.out.println( database1.bucketIDs.toString());
////        String   filename2 = "src/main/resources/data/coursesdate_added.ser";
////        GridIndex   database2 = null;
////        try {
////            // Reading the object from a file
////            FileInputStream file = new FileInputStream(filename2);
////            ObjectInputStream in = new ObjectInputStream(file);
////
////            // Method for deserialization of object
////            database2 = (GridIndex) in.readObject();
////
////            in.close();
////            file.close();
////            System.out.println("deserilized1");
////
////        } catch (IOException e) {
////            e.printStackTrace();
////        } catch (ClassNotFoundException e) {
////            e.printStackTrace();
////        }
////        System.out.println( database2.pathOfBuckets.toString());
////        System.out.println( database2.bucketIDs.toString());
////
//
//        String   filename3 = "src/main/resources/data/transcriptscourse_namedate_passed.ser";
//        GridIndex   database3 = null;
//        try {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream(filename3);
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            database3 = (GridIndex) in.readObject();
//
//            in.close();
//            file.close();
//            System.out.println("deserilized1");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.out.println( database3.pathOfBuckets.toString());
//        System.out.println( database3.bucketIDs.toString());
//        String   filename4 = "src/main/resources/data/transcriptsgpa.ser";
//        GridIndex   database4 = null;
//        try {
//            // Reading the object from a file
//            FileInputStream file = new FileInputStream(filename4);
//            ObjectInputStream in = new ObjectInputStream(file);
//
//            // Method for deserialization of object
//            database4 = (GridIndex) in.readObject();
//
//            in.close();
//            file.close();
//            System.out.println("deserilized1");
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.out.println( database4.pathOfBuckets.toString());
//        System.out.println( database4.bucketIDs.toString());
////
//
//
//
////
//
////        String tableName = "dogs";
////        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
////        htblColNameType.put("id", "java.lang.String");
////        htblColNameType.put("name", "java.lang.String");
////        htblColNameType.put("age", "java.lang.Integer");
////
////        Hashtable<String, String> minValues = new Hashtable<>();
////        minValues.put("id", "1");
////        minValues.put("name", "AAAAAA");
////        minValues.put("age", "1");
////
////        Hashtable<String, String> maxValues = new Hashtable<>();
////        maxValues.put("id", "5");
////        maxValues.put("name", "zzzzzz");
////        maxValues.put("age", "100");
////
////        db.createTable(tableName, "id", htblColNameType, minValues, maxValues);
//
//
//
//
//
////        Hashtable<String, Object> row = new Hashtable<>();
////        row.put("id","1");
////        row.put("name","Lacy");
////        row.put("age",2);
////        db.insertIntoTable("dogs", row);
////
////        Hashtable<String, Object> row2 = new Hashtable<>();
////        row2.put("id","2");
////        row2.put("name","Max");
////        row2.put("age",4);
////        db.insertIntoTable("dogs", row2);
////
////        Hashtable<String, Object> row3 = new Hashtable<>();
////        row3.put("id","3");
////        row3.put("name","Cony");
////        row3.put("age",10);
////        db.insertIntoTable("dogs", row3);
//////
////        Hashtable<String, Object> row4 = new Hashtable<>();
////        row4.put("id","4");
////        row4.put("name","Max");
////        row4.put("age",9);
////        db.insertIntoTable("dogs", row4);
//
//
////        System.out.println(db.PKdataType(tableName));
//
////        try {
////
////            File f = new File("src/main/resources/data/students1.ser");
////            //file to be delete
////            if (f.delete())                      //returns Boolean value
////            {
////
////                System.out.println(f.getName() + " deleted");   //getting and printing the file name
////            } else {
////                System.out.println("failed");
////            }
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
//        ArrayList<int []> x = new ArrayList();
//        x = ranges(1,24);
//        for(int i = 0 ; i<x.size() ; i++){
//            System.out.print(Arrays.toString(x.get(i)));
//        }
//        System.out.println();
//        int index = binarySearchOnRanges(x,20);
//        System.out.println(index);
//        makeIndexedTrue("courses" , "hours");
//        int x = getIndixfCol("courses" , "course_id");
//        System.out.println(x);
//        String pattern = "yyyy-MM-dd";
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

//        SimpleDateFormat DateFor = new SimpleDateFormat("yyyy-MM-dd");
////
//        try{
//            Date date = DateFor.parse("2020-01-22");
//            Date date2 = DateFor.parse("2024-10-22");
//            ArrayList<String []> x =  rangesDate(date, date2);
////             x =
//
//            Date dateR = DateFor.parse("2022-01-22");
////            System.out.println(date.getTime());
//            System.out.println(x.size());
//            for(int i = 0 ; i<x.size() ; i++){
//                System.out.print(Arrays.toString(x.get(i)));
//
//            }
//            System.out.println(binarySearchOnRangesDate(x , dateR));
////            System.out.println("Date : "+date);
//        }catch (ParseException e) {e.printStackTrace();}
//
//        System.out.println("hii");


//        ArrayList<String []> a = rangesDouble(0.5,15.5);
//        for(int i = 0 ; i<a.size() ; i++){
//            System.out.print(Arrays.toString(a.get(i)));
//        }
//
//        int x = binarySearchOnRangesDouble(a,5.1);
////        int x = binarySearchOnRangesString(a,"mohamed");
//        System.out.println(x);
//
//        System.out.println(dataTypeOfcol("courses" , "hours"));











