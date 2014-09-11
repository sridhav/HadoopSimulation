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
            
            // Runs the conventional Sort class
            Simulator ex=new Simulator();
            ex.runSimulation();
            slaves=ex.getSlaves();
            master=ex.getMaster();
            volume_bigdata=ex.getVolumeBigData();
            velocity_bigdata=ex.getVelocityBigData();
            variety_bigdata=ex.getVarietyBigData();
            
            //Runs the Big Data Sort class
            SimulatorBigData ex2=new SimulatorBigData(master,slaves,volume_bigdata,velocity_bigdata,variety_bigdata);
            ex2.runSimulation();
            
            System.out.println("Throughput Conventional Sort");
            System.out.println(ex.through);
            System.out.println("Throughput Big Data Sort");

            System.out.println(ex2.through);
            System.out.println("Speed Up");
            System.out.println((ex2.through-ex.through)/ex.through);
        
        
    }
}
