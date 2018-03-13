package com.anhouse.datahouse.logparser.mr.webh5;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**

 * 四个泛型类型分别代表：

 * KeyIn        Reducer的输入数据的Key
 * ValueIn      Reducer的输入数据的Value
 * KeyOut       Reducer的输出数据的Key
 * ValueOut     Reducer的输出数据的Value

 */

public class ParseWebh5LogReducer extends Reducer<Text, Text, NullWritable, Text> {
	
	private MultipleOutputs<LongWritable,Text> mos;

    @Override
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
        for (Text value : values) {
        	// 指定写出不同文件的数据 
        	mos.write("webh5", null, value,key.toString().split("!")[0]+"/");
        }
    }
	@Override 
	public void setup(Context context){	 
    
      mos=new MultipleOutputs(context);
      
    }
    @Override 
    public void cleanup(Context context) throws IOException, InterruptedException{
      mos.close();
    } 


}

