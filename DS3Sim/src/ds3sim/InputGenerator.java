/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ds3sim;

import java.io.FileWriter;
import java.io.IOException;

/**
 *
 * @author Sridhar
 */
public class InputGenerator {
    InputGenerator(int no_files,int no_slaves) throws IOException{
        runGen(no_files,no_slaves);
    }

    private void runGen(int no_files, int no_slaves) throws IOException {
        int count =0;
        for(int i=0;i<no_files;i++){
            FileWriter fw=new FileWriter("./inp/inp"+i+".txt",false);
           count=i+1;
            fw.write(i+"\n");
            fw.write("4000\n");
            fw.write(100+"\n");
            fw.write("500\n");
            fw.write("1030\n");
            fw.write("10\n");
            fw.write("20\n");
               
            fw.write(no_slaves+"\n");
            fw.write(no_slaves/2+"\n");
            for(int j=0;j<no_slaves;j++){
                fw.write("500\n");
                fw.write("1030\n");
                fw.write("c\n");
                fw.write("10\n");
                fw.write("20\n");
                fw.write("c\n");
            }
            fw.close();
        }
    }
    
}
