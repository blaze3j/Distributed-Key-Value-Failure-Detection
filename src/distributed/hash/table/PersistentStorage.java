package distributed.hash.table;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PersistentStorage {    
    void save(String fileName, Object object) throws IOException
    {
        // Use a FileOutputStream to send data to a file
        FileOutputStream fileOut = new FileOutputStream (fileName);
        // Use an ObjectOutputStream to send object data to the
        // FileOutputStream for writing to disk.
        ObjectOutputStream objOut = new ObjectOutputStream (fileOut);
        // Pass our object to the ObjectOutputStream's
        // writeObject() method to cause it to be written out
        // to disk.
        objOut.writeObject(object);
        
        objOut.close();
        fileOut.close();
    }
    
    Object load(String fileName) throws IOException, ClassNotFoundException 
    {
     // Read from disk using FileInputStream.
        FileInputStream fileIn = new FileInputStream (fileName);
        // Read object using ObjectInputStream.
        ObjectInputStream objIn = new ObjectInputStream (fileIn);
        // Read an object.
        Object obj = objIn.readObject();
        
        objIn.close();
        fileIn.close();
        return obj;
    }
    
}
