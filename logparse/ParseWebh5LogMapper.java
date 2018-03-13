package com.anhouse.datahouse.logparser.mr.webh5;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anhouse.datahouse.constant.Constant;
import com.anhouse.datahouse.logparser.weblog.ParseNginxLogParameter;
import com.anhouse.datahouse.logparser.weblog.ParseNginxLogUtils;
import com.anhouse.datahouse.ngnix.NginxLog;
import com.anhouse.datahouse.ngnix.UbiPageEvent;

public class ParseWebh5LogMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	protected final static Logger LOGGER = LoggerFactory.getLogger(ParseWebh5LogMapper.class);
	private ParseNginxLogUtils parseNginxLogUtils = new ParseNginxLogUtils();
	
	
	public static String dateStr=null;
	public static String pageEventDesFilePath=null;
	public static String pageDataDesFilePath=null;
	private static String errorLinePath=null;
	
	

	public ParseWebh5LogMapper() {
		
	}
	
    @Override
	public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

    	if (dateStr==null) {
    		dateStr=context.getConfiguration().get("dateStr");
		}
    	if (pageEventDesFilePath==null) {
    		pageEventDesFilePath=context.getConfiguration().get("pageEventDesFilePath");
		}
    	if (pageDataDesFilePath==null) {
    		pageDataDesFilePath=context.getConfiguration().get("pageDataDesFilePath");
		}
    	if (errorLinePath==null) {
    		errorLinePath=context.getConfiguration().get("errorLinePath");
		}
    	
	    parseWebh5Log(key,value.toString(),context);
	    
	}

	protected void parseWebh5Log(LongWritable key,String line,Context context) {
		
		// 定义接受数据的列表
		List<String> errorLines=null;
		errorLines = new LinkedList<String>();
		try {
			ParseNginxLogParameter.PARSE_DATE_NOW=dateStr;
			shuntLogRecord(key,line, context,dateStr, errorLines);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.info("---------------解析出错--------------->数据：" + line);
			LOGGER.info("错误信息：" + e.getMessage());
		}
				
	}

	/**
	 * 根据条件对日志数据进行分流
	 * 
	 * @param line
	 *            一条原始日志数据
	 * @param date
	 *            日志产生的日期
	 * @param pageEventList
	 *            保存的是要存入o_ubi_page_event的数据 
	 * @param pageDataList
	 *            保存的是要存入o_ubi_page_data的数据
	 * @param errorLines
	 *            保存解析有异常或不满足上面的条件的记录
	 * @throws InterruptedException 
	 * @throws IOException 
	 */

	public void shuntLogRecord(LongWritable key,String line,Context context, String date, List<String> errorLines) throws Exception {
		NginxLog log = parseNginxLogUtils.parseNginxLogAll(line, errorLines);
		if (log != null) {
			boolean choose = parseNginxLogUtils.chooseRule(log, date);
			int classify = parseNginxLogUtils.classifyLogRecord(log, choose);
			UbiPageEvent event = parseNginxLogUtils.getUbiPageEvent(log, line, errorLines);
			if (date.compareTo(ParseNginxLogParameter.SET_WTID_WTDATA_NULL_DATE) <= 0) {
				event.setWt_id(null);
				event.setWt_data(null);
			}
			String uuid=UUID.randomUUID().toString();
			if (classify == Constant.PARSE_O_UBI_PAGE_EVENT) {
			    if (!parseNginxLogUtils.needToReject(event)) {
			    	//自定义counter进行统计
			    	context.getCounter("webh5", "PAGE_EVENT_COUNT").increment(1);
	            	context.write(new Text(pageEventDesFilePath+"!"+uuid),new Text(parseNginxLogUtils.handleSpecialChar(event.toString(), "null", Constant.HIVE_SCORE_NULL)));
		        }
				
			} else if (classify == Constant.PARSE_O_UBI_PAGE_DATA) {
			    if (!parseNginxLogUtils.needToReject(event)) {
			    	context.getCounter("webh5", "PAGE_DATA_COUNT").increment(1);
	            	context.write(new Text(pageDataDesFilePath+"!"+uuid),new Text(parseNginxLogUtils.handleSpecialChar(event.toString(), "null", Constant.HIVE_SCORE_NULL)));
		        }
			} else {
				context.getCounter("webh5", "NO_USE_COUNT").increment(1);
				context.write(new Text(errorLinePath+"!"+uuid),new Text(parseNginxLogUtils.handleSpecialChar(event.toString(), "null", Constant.HIVE_SCORE_NULL)));
			}
			
		}
	}
   

}
