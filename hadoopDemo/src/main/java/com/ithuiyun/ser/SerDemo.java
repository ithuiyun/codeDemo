package com.ithuiyun.ser;

import org.apache.hadoop.io.Writable;

import java.io.*;

/**
 * Created by ithuiyun.com on 2018/4/29.
 */
public class SerDemo {
    public static void main(String[] args) throws Exception{
        //writeJava();
        writeHadoop();

    }

    private static void writeHadoop() throws IOException {
        StudentWritable studentWritable = new StudentWritable();
        studentWritable.setId(1L);
        studentWritable.setName("徽云学院");

        FileOutputStream fos = new FileOutputStream("d:\\b.txt");
        DataOutputStream oos = new DataOutputStream(fos);
        studentWritable.write(oos);
        oos.close();
        fos.close();
    }

    private static void writeJava() throws IOException {
        StudentJava studentJava = new StudentJava();
        studentJava.setId(1L);
        studentJava.setName("徽云学院");

        FileOutputStream fos = new FileOutputStream("d:\\a.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(studentJava);
        oos.close();
        fos.close();
    }


}
class StudentJava implements Serializable{
    private static final long serialVersionUID = 1L;
    private Long id;
    private String name;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

class StudentWritable implements Writable{
    private Long id;
    private String name;
    public Long getId() {
        return id;
    }
    public void setId(Long id) {
        this.id = id;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.id);
        out.writeUTF(this.name);
    }
    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
