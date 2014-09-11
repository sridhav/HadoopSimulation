/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ds3sim;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author Sridhar
 */
public class DS3Sim {

    /**
     * @param args the command line arguments
     */
    private static List<SlaveNode> slaves;
    private static SlaveNode master;
    private static double volume_bigdata;
    private static long velocity_bigdata;
    private static long variety_bigdata;
    
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        int no_of_files=50;
        InputGenerator x=new InputGenerator(no_of_files,50);
         FileWriter fw=new FileWriter("./out.csv");
           
        for(int i=0;i<no_of_files;i++){
            String temp="./inp/inp"+i+".txt";
            Simulator ex=new Simulator(temp);
            ex.runSimulation();
            slaves=ex.getSlaves();
            master=ex.getMaster();
            volume_bigdata=ex.getVolumeBigData();
            velocity_bigdata=ex.getVelocityBigData();
            variety_bigdata=ex.getVarietyBigData();
            SimulatorBigData ex2=new SimulatorBigData(master,slaves,volume_bigdata,velocity_bigdata,variety_bigdata);
            ex2.runSimulation();
            fw.write(i+","+ex.through+","+ex2.through+","+((ex2.through-ex.through)/ex.through)+"\n");
        }
         fw.close();
    }
}
