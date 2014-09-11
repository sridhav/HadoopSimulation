/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ds3sim;

/**
 *
 * @author Sridhar
 */
public class SlaveNode {
    
    public long node_mem_high=0;
    public long node_mem_low=0;
    public long node_cpu_high=0;
    public long node_cpu_low=0;
    
    public String isCluster_mem;
    public String isCluster_cpu;
    
    public long node_mem_avg=0;
    public long node_cpu_avg=0;
    
    public long node_cpu_processing=0;
    
    public long node_transfer_rate;
    public long mem_to_sort_conv=0;
    
    public long conv_sort_time=0;
    
    public long slave_ret_sort_conv=0;
    public double slave_ratio=0;
    public long my_split_value;
    long partitions_total;
    long no_of_longs;
    double singlePartitionSort;
    double allPartitionSort;
    long mergePartitions;
    long no_of_maps;
    int no_nodes;
    double newSort;
    double wordCount;
}
