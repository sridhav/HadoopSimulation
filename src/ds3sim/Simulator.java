/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ds3sim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author Sridhar
 */
public class Simulator {
    private double volume_bigdata;
    private long velocity_bigdata;
    private long variety_bigdata;
    private int no_slaves;
    private SlaveNode master=new SlaveNode();
    private List<SlaveNode> slaves=new LinkedList<SlaveNode>();
    private int nodes_connectivity;
    public double through;
    
    public Simulator() throws IOException{
        readValues();
        
    }

    //Reads the values from the input
    
    private void readValues() throws IOException {
         //File x=new File(file);
         long temp2;
        BufferedReader bw=new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Volume( Enter the volume of the big data in TB) :");
        volume_bigdata=Double.parseDouble(bw.readLine());
        volume_bigdata=volume_bigdata*1024*1024*1024*1024*8;
        System.out.println("Velocity( Enter the velocity of the big data in bps) :");
        velocity_bigdata=Long.parseLong(bw.readLine());
        System.out.println("Variety( Enter the mix of bigdata, the format to be determined by you) :");
        variety_bigdata=Long.parseLong(bw.readLine());
        
        System.out.println();
        System.out.println("Master Node Memory");
        System.out.println("Enter the low end in GB");
        temp2=Long.parseLong(bw.readLine());
        master.node_mem_low=temp2*1024*1024*1024*8;
        System.out.println("Enter the high end in GB");
        temp2=Long.parseLong(bw.readLine());
        master.node_mem_high=temp2*1024*1024*1024*8;
        master.node_mem_avg=(master.node_mem_high+master.node_mem_low)/2;
       
        System.out.println("Master Node CPU");
        System.out.println("Enter the low end in GHz");
        temp2=Long.parseLong(bw.readLine());
        master.node_cpu_low=temp2*1000*1000*1000;
        System.out.println("Enter the high end in GHz");
        temp2=Long.parseLong(bw.readLine());
        master.node_cpu_high=temp2*1000*1000*1000;
        master.node_cpu_avg=(master.node_cpu_high+master.node_cpu_low)/2;
        master.node_transfer_rate=master.node_cpu_avg/4;
        master.node_cpu_processing=master.node_cpu_avg/2;
        System.out.println("Enter the Number of slaves(1-n)");
        no_slaves=Integer.parseInt(bw.readLine());
        
        System.out.println("Enter the connectivity");
        nodes_connectivity=Integer.parseInt(bw.readLine());
        for(int i=0;i<no_slaves;i++){
            SlaveNode temp=new SlaveNode();
            System.out.println("Slave Node Memory");
            System.out.println("Enter the low end in GB");
            temp2=Long.parseLong(bw.readLine());
            temp.node_mem_low=temp2*1024*1024*1024*8;
            System.out.println("Enter the high end in GB");
            temp.node_mem_high=Long.parseLong(bw.readLine());
            temp.node_mem_high=temp.node_mem_high*1024*1024*1024*8;
            System.out.println("Random r Clustered c");
            temp.isCluster_mem=bw.readLine();
            temp.node_mem_avg=(temp.node_mem_high+temp.node_mem_low)/2;
      
            
            System.out.println("Slave Node CPU");
            System.out.println("Enter the low end in GHz");
            temp.node_cpu_low=Long.parseLong(bw.readLine());
            temp.node_cpu_low=temp.node_cpu_low*1000*1000*1000;
            System.out.println("Enter the high end in GHz");
            temp.node_cpu_high=Long.parseLong(bw.readLine());
            temp.node_cpu_high=temp.node_cpu_high*1000*1000*1000;
            System.out.println("Random r Clustered c");
            temp.isCluster_cpu=bw.readLine();
            temp.node_cpu_avg=(temp.node_cpu_high+temp.node_cpu_low)/2;
             temp.node_transfer_rate=temp.node_cpu_avg/4;
             temp.node_cpu_processing=temp.node_cpu_avg/2;
            slaves.add(temp);
        }
        bw.close();
    }
    
    
    // Runs the simulation
    
    public void runSimulation() throws IOException{
        boundryConditions();
        slaves=sortNodes(slaves);
        //sends data to the nodes
        dataSet();
        
        //Experimenting using the ratios
        //setRatios();
        //splitMem();
        
        //Running the conventional Sort
        runConvSort();
       // getSeqSortTime();
       // displayConfig();
    }

    private void runConvSort() throws IOException {
        //gets the total sort Time
        getTotSortTime_conv();
        //obtains throughput
        throughput();
        
    }

    //Transfer Time is ignored But code has been written
    private long getTotTransferTime_conv() {
       
        long transferTime=0;
       /* if(master_mem>=(volume_bigdata)){
            transferTime=(volume_bigdata)/(velocity_bigdata);
        }
        else{
            long remainMem=volume_bigdata-master_mem;
            transferTime=transferTime+((master.node_mem_avg/32)/master.node_transfer_rate);
            for(int i=0;i<slaves.size();i++){
                if(remainMem>0){
                    if(remainMem>slaves.get(i).node_mem_avg){
                        remainMem=remainMem-slaves.get(i).node_mem_avg;
                        transferTime=transferTime+((slaves.get(i).node_mem_avg/32)/slaves.get(i).node_transfer_rate);
                    }
                    else{
                        transferTime=transferTime+((remainMem/32)/slaves.get(i).node_transfer_rate);
                    }
                }
            }
        }*/
        transferTime=(long) (volume_bigdata/velocity_bigdata);
        return transferTime;
    }

    //Obtains the total Sort Time
    private void getTotSortTime_conv() {
           //Individual Sort Timings
            long tempVar=getSortValue(master.mem_to_sort_conv);
            master.conv_sort_time=(tempVar)/master.node_cpu_processing;
           for(int i=0;i<slaves.size();i++){
               tempVar=getSortValue(slaves.get(i).mem_to_sort_conv);
               slaves.get(i).conv_sort_time=(tempVar)/slaves.get(i).node_cpu_processing;
           }
     }

    //Obtains Max Sort Time among the individual Sort Timings
    private long getMaxSortTime_conv(int f,int l) {
        long max=master.conv_sort_time;
        for(int i=f;i<l;i++){
            if(max<slaves.get(i).conv_sort_time){
                max=slaves.get(i).conv_sort_time;
            }
        }
        return max;
    }

    // Return time is also ignored as the its really very minute but code is written
    private long getTotRetTime_conv() {
        long retTime=0;
        for(int i=0;i<slaves.size();i++){
            slaves.get(i).slave_ret_sort_conv=(slaves.get(i).mem_to_sort_conv/64)/slaves.get(i).node_transfer_rate;
           /* if(retTime<slaves.get(i).slave_ret_sort_conv){
                retTime=slaves.get(i).slave_ret_sort_conv;
            }*/
            retTime=retTime+slaves.get(i).slave_ret_sort_conv;
        }
        return retTime;
    }

    // Merge Run conventional Experimented Not used
    private long getMergeRun_conv() {
        long mergeTime=0;
        long sum=0;
        for(int i=0;i<slaves.size();i++){
            sum=sum+slaves.get(i).mem_to_sort_conv;
        }
        long temp=(5*no_slaves+2);
        mergeTime=((sum*temp)/64)/master.node_cpu_processing;
        return mergeTime;
    }

    // For debugging Purposes
    private void displayConfig() {
         System.out.println("###MASTER###");
            System.out.println(master.node_mem_high);
            System.out.println(master.node_mem_low);
            System.out.println(master.node_mem_avg);
            System.out.println(master.node_cpu_high);
            System.out.println(master.node_cpu_low);
            System.out.println(master.node_cpu_avg);
            System.out.println(master.mem_to_sort_conv);
            
            System.out.println("##############");
        for(int i=0;i<slaves.size();i++){
            System.out.println("#######"+i+"######");
            System.out.println(slaves.get(i).node_mem_high);
            System.out.println(slaves.get(i).node_mem_low);
            System.out.println(slaves.get(i).node_mem_avg);
            System.out.println(slaves.get(i).node_cpu_high);
            System.out.println(slaves.get(i).node_cpu_low);
            System.out.println(slaves.get(i).node_cpu_avg);
            System.out.println(slaves.get(i).mem_to_sort_conv);
            System.out.println("##############");
        }
    }
    
    // ** important function Obtains the sort time for every single node
    private long getSortValue(long node_mem_avg) {
        
        //here the running time is equivalent to (11n+6)log(11n+6) + 6
          long temp=(((node_mem_avg)/64)*11)+6;
          double temp2=Math.log(temp)/Math.log(2);
          temp=(long) ((long) (temp*temp2));
          return temp;
    }
    
    public List<SlaveNode> getSlaves(){
        return slaves;
    }
    public SlaveNode getMaster(){
        return master;
    }
    public double getVolumeBigData(){
        return volume_bigdata;
    }
    public long getVelocityBigData(){
        return velocity_bigdata;
    }
    public long getVarietyBigData(){
        return variety_bigdata;
    }
    
    
    // if the input data is greater than that of the slave nodes an exception is returned
     private void boundryConditions() {
        long sum=master.node_mem_high;
        for(int i=0;i<slaves.size();i++){
            sum=sum+slaves.get(i).node_mem_high;
        }
        if(sum<volume_bigdata){
            System.out.println("Not Enough Number to slaves to Process Sorting");
            System.exit(1);
        }
    
    }
     
     
    // Sorts the nodes using Quick Sort according to their size 
    private List<SlaveNode> sortNodes(List<SlaveNode> arr) {
          if(!arr.isEmpty()){
            SlaveNode temp=arr.get(arr.size()/2);
            long pivot=temp.node_mem_high;
            List<SlaveNode> leftArr=new LinkedList<SlaveNode>();
            List<SlaveNode> rightArr=new LinkedList<SlaveNode>();
            List<SlaveNode> equalArr=new LinkedList<SlaveNode>();
            for(SlaveNode i:arr){
                long j=i.node_mem_high;
                if(j<pivot){
                    leftArr.add(i);
                }
                else if(j>pivot){
                    rightArr.add(i);
                }
                else{
                    equalArr.add(i);
                }
            }
            leftArr=sortNodes(leftArr);
            rightArr=sortNodes(rightArr);
            
            rightArr.addAll(equalArr);
            rightArr.addAll(leftArr);
            return rightArr;
        }
        return arr;
    } 

    // When nodes connected in sequential
    private double getSeqSortTime() {
            //Obtaining Max Sort Time in Sequential
            double max=0;
            for(int i=0;i<nodes_connectivity;i++){
                if(max<slaves.get(i).conv_sort_time){
                    max=slaves.get(i).conv_sort_time;
                }
            }
            
            long sum=0;
            double newSortTime=0;
            double transferTime=0;
            //Merge (11n+6)
            //TransferTime is not considered
           for(int i=(int) (nodes_connectivity)-2;i>=0;i--){
               sum=sum+(slaves.get(i).mem_to_sort_conv/64);                 
               transferTime=transferTime+(sum/64)/slaves.get(i).node_transfer_rate;
               newSortTime=newSortTime+(((11*sum)+6)/(slaves.get(i).node_cpu_processing));
           }
           //Merge with master cancelled with master
          // sum=sum+(master.mem_to_sort_conv/64);
           //newSortTime=newSortTime+((((11*sum)+6)/(master.node_cpu_processing)));
           return (newSortTime+max);
           
    }
    
    //When Nodes connected in parallel
    private double getParSortTime(){
        double parSortTime=0;
        double max=0;
        double transferToMaster=0;
        double max2=0;
        double no_longs_to_merge=0;
        double mergeTime=0;
        int count=0;
        //ObtainingMaxSortTime in Parallel
        for(int i=(int)nodes_connectivity;i<no_slaves;i++){
            if(max<slaves.get(i).conv_sort_time){
                max=slaves.get(i).conv_sort_time;
            }
            //TransferRate to Master
            transferToMaster=(slaves.get(i).mem_to_sort_conv/64)/slaves.get(i).node_transfer_rate;
            if(max2<transferToMaster){
                max2=transferToMaster;
            }
            //Merging Longs Total Longs to be merged
            no_longs_to_merge=no_longs_to_merge+(slaves.get(i).mem_to_sort_conv/64);
            count++;
        }
        
        //Merge Time
        //6+4n+4n+(no_of_nodes*7n)
       long temp=(long) (count*7*no_longs_to_merge);
       mergeTime=((4*no_longs_to_merge)+(4*no_longs_to_merge)+(temp))/master.node_cpu_processing;
        
        //Total Time
        //parSortTime=mergeTime+Max(TransferTime)+Max(SortTimes);
        
     parSortTime=mergeTime+max;
     return parSortTime;
        
    }
    
    
    // Merging all the sequential Parallel and Master Node values
    private double getMergeSeqParMas(){
        double totSortTime=0;
        double transferTime=0;
        double mergeTime=0;
        long no_of_longs=0;
        long masterSort=master.conv_sort_time;
        double maxTime=Math.max((double)masterSort, getSeqSortTime());
        maxTime=Math.max(maxTime, getParSortTime());
        
        for(int i=0;i<nodes_connectivity;i++){
            no_of_longs=no_of_longs+(slaves.get(i).mem_to_sort_conv/64);
        }
        //Transfer from Sequential top nodes
        transferTime=no_of_longs/slaves.get(0).node_transfer_rate;
        
        //Merging all Three
        //6+4n+4n+3*7n
        mergeTime=((4*no_of_longs)+(4*no_of_longs)+(3*7*no_of_longs))/master.node_cpu_processing;
        
        totSortTime=transferTime+mergeTime+maxTime;
        return totSortTime;
    }
    
    //Assigns the data to all the nodes
    private void dataSet() {
        long master_mem=master.node_mem_high;
        if(master_mem>=volume_bigdata){
            master.mem_to_sort_conv=(long) volume_bigdata;
        }
        else{
           long remainMem=(long) (volume_bigdata-master_mem);
           for(int i=0;i<slaves.size();i++){
               if(remainMem>0){
                   if(remainMem>slaves.get(i).node_mem_high){
                       remainMem=remainMem-slaves.get(i).node_mem_high;
                       slaves.get(i).mem_to_sort_conv=slaves.get(i).node_mem_high;
                   }
                   else{
                       slaves.get(i).mem_to_sort_conv=remainMem;
                       break;
                   }
               }
           }
        }
    
    }
    
    
    //Experimented used in the no of slaves vs throughput graph
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
    //Experimented used in the no of slaves vs throughput graph
    private void splitMem() {
        for(int i=0;i<slaves.size();i++){
            slaves.get(i).mem_to_sort_conv=Math.round(volume_bigdata*slaves.get(i).slave_ratio);
        }
    }
    
    //Throughput function 
    private double throughput(){
        double output=0;
        output=(getSeqSortTime()+getParSortTime()+getMergeSeqParMas());
        output=volume_bigdata/output;
        through=output/(1024*1024*8);
        return output/(1024*1024*8);
    }
}
