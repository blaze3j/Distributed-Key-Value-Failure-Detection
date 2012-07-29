package distributed.hash.table;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;

public class PersistentStorage {
    /** 
     * save object to file
     */
    void save(String fileName, Object object) throws IOException
    {
        // Use a FileOutputStream to send data to a file
        FileOutputStream fileOut = new FileOutputStream (fileName + ".tmp");
        // Use an ObjectOutputStream to send object data to the
        // FileOutputStream for writing to disk.
        ObjectOutputStream objOut = new ObjectOutputStream (fileOut);
        // Pass our object to the ObjectOutputStream's
        // writeObject() method to cause it to be written out
        // to disk.
        objOut.writeObject(object);
        
        objOut.close();
        fileOut.close();
        
        File file = new File(fileName + ".tmp");
        File file2 = new File(fileName);

        // Rename file (or directory)
        boolean success = file.renameTo(file2);
        if (!success) {
            throw new IOException();
        }
    }
    
    /** 
     * load hashtable from file
     */
    @SuppressWarnings("unchecked")
    Hashtable<String, ArrayList<String>> load(String fileName) throws IOException, ClassNotFoundException 
    {
     // Read from disk using FileInputStream.
        FileInputStream fileIn = new FileInputStream (fileName);
        // Read object using ObjectInputStream.
        ObjectInputStream objIn = new ObjectInputStream (fileIn);
        // Read an object.
        Object obj = objIn.readObject();
        Hashtable<String, ArrayList<String>> hashtable = null;
        if (obj instanceof Hashtable) {
            hashtable = (Hashtable<String, ArrayList<String>>) obj; 
        }
        else {
            throw new ClassNotFoundException();
        }
        objIn.close();
        fileIn.close();
        return hashtable;
    }
    
}
