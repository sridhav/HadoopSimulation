package ds3sim;

import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Sridhar
 */
public class SimulatorBigData {
    private double volume_bigdata;
    private long velocity_bigdata;
    private long variety_bigdata;
    private int no_slaves;
    private SlaveNode master;
    private List<SlaveNode> slaves;
    private long nodes_connectivity;
    private long chunkSize=64*1024*1024*8;
    private long maxMapVal;
    public double through;
    SimulatorBigData(SlaveNode master, List<SlaveNode> slaves, double volume_bigdata, long velocity_bigdata, long variety_bigdata) {
        this.slaves=slaves;
        this.master=master;
        this.volume_bigdata=volume_bigdata;
        this.velocity_bigdata=velocity_bigdata;
        this.variety_bigdata=variety_bigdata;
        checkVariety();
    }
    
    // runs the simulation
    public void runSimulation() throws IOException{
        long transferTime=getTransferTime();
        boundryConditions();
        //assigns the data sizes to the slaves
        setRatios();
        //assigns the data sizes to the slaves
        splitMem();
        //Sets the partitions
        setPartitionNo();
        //Throughput is obtained
        throughput();
        
    }
    //transfer time is not considered
    private long getTransferTime() {
        long transTime;
        transTime=(long) ((volume_bigdata)/velocity_bigdata);
        return transTime;
    }

    //Individual sort times Imp Function internal Partition sort in memory sort
    private double getSortTimes() {
        double max=0;
        long maxMaps=0;
        for(int i=0;i<slaves.size();i++){
            slaves.get(i).no_of_maps=slaves.get(i).my_split_value/chunkSize;
            slaves.get(i).no_nodes=slaves.size();
            double temp=11*(slaves.get(i).no_of_longs/slaves.get(i).no_nodes)+6;
            double temp2=Math.log(temp)/Math.log(2);
            slaves.get(i).newSort=slaves.get(i).no_of_maps*(temp*temp2)/slaves.get(i).node_cpu_processing;
            if(max<slaves.get(i).newSort){
                max=slaves.get(i).newSort;
            }
            if(maxMaps<slaves.get(i).no_of_maps){
                maxMaps=slaves.get(i).no_of_maps;
            }
        }
        maxMapVal=maxMaps;
        return max;
    
    }
    
    //Assigns data
    private void setRatios() {
          long sum=0;
          for(int i=0;i<slaves.size();i++){
             long mem=slaves.get(i).node_mem_high;
              sum=sum+mem;
          }
          for(int i=0;i<slaves.size();i++){
              slaves.get(i).slave_ratio=((float)(slaves.get(i).node_mem_high)/sum);
          }
   }
    //Assigns data sizes to the slaves
    private void splitMem() {
        for(int i=0;i<slaves.size();i++){
            slaves.get(i).my_split_value=Math.round(volume_bigdata*slaves.get(i).slave_ratio);
        }
    }
    
    // Partitions are generated
    private void setPartitionNo() {
        for(int i=0;i<slaves.size();i++){     
            long temp=slaves.get(i).my_split_value;
            long x=temp/chunkSize;
            slaves.get(i).partitions_total=(long)Math.ceil(x);
            slaves.get(i).no_of_longs=(long)Math.ceil(chunkSize/64);
        }
    }

    //If volume of big data is greater than the total size of slaves exception is raised
    private void boundryConditions() {
        long sum=0;
        for(int i=0;i<slaves.size();i++){
            sum=sum+slaves.get(i).node_mem_high;
        }
        if(sum<volume_bigdata){
            System.out.println("Not Enough Number to slaves to Process Sorting");
            System.exit(1);
        }
    
    }
    // Merging of partiions in every single node Imp function
    //NM*NL*NM
    private double mergingProc() {
        double mergeTime=0;
        double max=0;
        for(int i=0;i<slaves.size();i++){
            long temp=(slaves.size()*5)+2;
            mergeTime=((slaves.get(i).no_of_maps*slaves.get(i).no_of_longs*temp)/slaves.get(i).node_cpu_processing);
            if(max<mergeTime){
                max=mergeTime;
            }
        }
        return max;
    }
    
    //data to the slaves
    private double dataRetTime(){
        double max=0;
        double retTime=0;
        for(int i=0;i<slaves.size();i++){
            retTime=((slaves.get(i).no_of_maps-1)*slaves.get(i).no_of_longs)/slaves.get(i).node_transfer_rate;
            if(max<retTime){
                max=retTime;
            }
        }
        return max;
    }
     
    //Word Count algorithm for the partition
    private double wordCountAlg(){
        double mergeTime=0;
        double retTime=dataRetTime();
        double max=0;
        double wordCountTime=getWordCountTimes();
        for(int i=0;i<slaves.size();i++){
            mergeTime=((slaves.get(i).no_of_maps*slaves.get(i).no_of_longs*slaves.size())/slaves.get(i).node_cpu_processing);
            if(max<mergeTime){
                max=mergeTime;
            } 
        }
        return (mergeTime+wordCountTime+retTime);
    }
    
    //Word Count Extended
    private double getWordCountTimes() {
        double max=0;
        for(int i=0;i<slaves.size();i++){
            double temp=2*(slaves.get(i).no_of_longs/slaves.get(i).no_nodes)+10;
            slaves.get(i).wordCount=slaves.get(i).no_of_maps*(temp)/slaves.get(i).node_cpu_processing;
            if(max<slaves.get(i).wordCount){
                max=slaves.get(i).wordCount;
            }
        }
        return max;
    }
    
    //Throughput
    private double throughput(){
        double output=0;
        output=(getSortTimes()+mergingProc()+dataRetTime()+wordCountAlg()+addSequential());
        output=volume_bigdata/output;
        through=output/(1024*1024*8);
        return output/(1024*1024*8);
    }
    
    
    //If nodes are connected in sequential
    private double addSequential(){
        double temp=0;
        for(int i=0;i<nodes_connectivity;i++){
            
            double temp2=(slaves.get(i).mem_to_sort_conv/64);
            double temp3=Math.log(temp2)/Math.log(2);
            double temp4=11*temp2*temp3;
            if(temp<temp4){
                temp=temp4;
            }
        }
        return temp;
    }
    
    
    //Variety is considered
    private void checkVariety(){
        if(variety_bigdata==1){
            chunkSize=2*1024*1024*8;
        }
        else if(variety_bigdata==2){
            chunkSize=4*1024*1024*8;
        }
        else if(variety_bigdata==3){
            chunkSize=8*1024*1024*8;
        }
        else if(variety_bigdata==4){
            chunkSize=16*1024*1024*8;
        }
        else if(variety_bigdata==5){
            chunkSize=32*1024*1024*8;
        }
         else if(variety_bigdata==6){
            chunkSize=64*1024*1024*8;
        }
        else if(variety_bigdata==7){
            chunkSize=128*1024*1024*8;
        }
        else if(variety_bigdata==8){
            chunkSize=256*1024*1024*8;
        }
        else{
            chunkSize=64*1024*1024*8;
        }
    }
}
