package com.anhouse.datahouse.logparser.mr.webh5;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.anhouse.datahouse.common.util.ResourceUtil;
import com.anhouse.datahouse.constant.Constant;
import com.anhouse.datahouse.etl.mapper.QuartzjobResultMapper;
import com.anhouse.datahouse.logparser.weblog.ParseNginxLogParameter;
import com.anhouse.datahouse.logparser.weblog.ParseNginxLogUtils;
import com.anhouse.datahouse.po.Config;
import com.anhouse.datahouse.po.DHQuartzJobResult;
import com.anhouse.datahouse.util.DateUtils;
import com.anhouse.datahouse.util.HiveConnection;
import com.anhouse.datahouse.util.KafkaUtil;
import com.anhouse.datahouse.util.MathUtil;
import com.anhouse.datahouse.util.MysqlUtils;
import com.anhouse.datahouse.util.PropertyUtil;
import com.google.gson.Gson;
import com.xxx.util.DateUtil;
import com.xxx.util.hadoop.HadoopFileUtil;
import com.xxx.util.hive.HiveUtils;

public class ParseWebh5LogMR {
	
	private static Logger log = Logger.getLogger(ParseWebh5LogMR.class);
	private static PropertyUtil props = PropertyUtil.getInstance();
	private static ParseNginxLogUtils parseNginxLogUtils = new ParseNginxLogUtils();
	public static String outputPath = props.getProperty("mysqldata_path");
	private HiveUtils hiveUtils;
	private final String jobName = "ParseNginxLogFromHdfsToHiveProcessor";
	private boolean isHistory;
	private static FileSystem fileSystem = null;
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	private static String startDate = DateUtil.getAgoBackDate(-1);
	

	public static void main(String[] args) {
		
		String sql = "select * from xxx_quartzjob_result where jobname= 'syncfileindicate' and jobProcessCode='0000' and DATE_FORMAT(from_unixtime(iCreateTime),'%Y-%m-%d')=?";
		List<Map<String, Object>> rlist = MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db"))
				.getJdbcUtils().getResultSet(sql, DateUtil.getToday());
		boolean transFlag = false;// 当日日志是否已传输完毕
		if (rlist.size() > 0) {
			transFlag = true;
		}
		
		if (args.length>0) {
			MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db")).getJdbcUtils().excuteSql("delete from xxx_quartzjob_result where jobname='ParseNginxLogFromHdfsToHiveProcessor' and DATE_FORMAT(from_unixtime(iCreateTime),'%Y-%m-%d')=?", DateUtil.getToday());
			//删除已成功纪录，若在进行中，不删除
			MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db")).getJdbcUtils().excuteSql("delete from xxx_sceduler_detaillist  where scedulerid=10000001 and iStatus = 4 and date=?", DateUtil.getToday());
		}

		String sql2 = "select * from xxx_quartzjob_result where jobname = 'ParseNginxLogFromHdfsToHiveProcessor' and DATE_FORMAT(from_unixtime(iCreateTime),'%Y-%m-%d')=?";
		List<Map<String, Object>> elist = MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db"))
				.getJdbcUtils().getResultSet(sql2, DateUtil.getToday());
		boolean execFlag = false; // 是否已执行过
		if (elist.size() > 0) {
			execFlag = true;
		}
		
		String sql3 = "select *  from xxx_sceduler_detaillist where scedulerid=10000001 and iStatus=? and date=?";
		List<Map<String, Object>> runlist = MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db"))
				.getJdbcUtils().getResultSet(sql3,com.anhouse.datahouse.sceduler.constant.Constant.SCEDULERDETAIL_RUNNING,DateUtil.getToday());
		boolean runingFlag = false; // 是否正在执行
		if (runlist.size() > 0) {
			runingFlag = true;
		}
		

		System.setProperty("HADOOP_USER_NAME", "hdfs");
		long start = System.currentTimeMillis();

	    // 添加具有detailid为10000001的此任务记录
		String dSql="INSERT INTO xxx_sceduler_detaillist VALUES(NULL,10000001,3,1,0,1,NULL,NULL,?,UNIX_TIMESTAMP(),UNIX_TIMESTAMP())";
		log.info("transFlag:"+transFlag+"	execFlag:"+execFlag+"	runingFlag:"+runingFlag);
		if (args.length == 0) {
			if (transFlag && !execFlag && !runingFlag) { // 日志已传输完毕且未被执行
				
				MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db")).getJdbcUtils().excuteSql(dSql, DateUtil.getToday());
				
				new ParseWebh5LogMR().execute(startDate, startDate);
						
			}
		} else {
			
			String[] dateResult=validDate(args);
			log.info("dateResult:"+Arrays.toString(dateResult));
			if (!execFlag && !runingFlag) {
				MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db")).getJdbcUtils().excuteSql(dSql, DateUtil.getToday());
				new ParseWebh5LogMR().execute(dateResult[0], dateResult[1]);
			}
		}
		
		String usql = "update xxx_sceduler_detaillist set iStatus = ? where scedulerid=10000001 and date=?";
		MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db"))
				.getJdbcUtils().excuteSql(usql, com.anhouse.datahouse.sceduler.constant.Constant.SCEDULERDETAIL_SUCCESS,DateUtil.getToday());
		
		long interval = (System.currentTimeMillis() - start) / 1000;
		if (interval < 60) {
			System.out.println("all ：" + interval + " s");
		} else {
			System.out.println("all ：" + interval / 60 + " m");
		}
		
	}

	/**
	 * 校验输入日期格式是否正确
	 * 
	 * @param date
	 * @return
	 */
	public static boolean checkDateFormat(String date) {
		boolean flag = true;
		try {
			sdf.parse(date);
		} catch (ParseException e) {
			log.info(date + "日期格式输入错误!\n" + e.getMessage());
			flag = false;
		}
		return flag;
	}
    
	public static String[] validDate(String[] args) {

		String[] result =new String[2]; 
		if (args.length == 1) {
			startDate = args[0];
			String endDate = startDate;
			if (!checkDateFormat(startDate)) {
				log.info("日期格式异常");
				System.exit(500);
			}
			result[0]=startDate;
			result[1]=endDate;
		} else if (args.length == 2) {

			startDate = args[0];
			String endDate = args[1];
			if (!checkDateFormat(startDate) || !checkDateFormat(endDate)) {
				log.info("日期格式异常");
				System.exit(500);
			}
			try {
				if (sdf.parse(startDate).after(sdf.parse(endDate))) {
					log.info("开始日期不能大于结束日期");
					System.exit(500);
				}
				if (sdf.parse(endDate).after(sdf.parse(DateUtil.getToday()))) {
					endDate=DateUtil.getToday();
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
			result[0]=startDate;
			result[1]=endDate;
		} else if (args.length > 2) {
			log.info("参数输入超过正常数量-->" + args.length + " [" + args + "]");
			System.exit(500);
		}
		return result;
	}

	public ParseWebh5LogMR() {

		fileSystem = getFileSystem();
	}

	public void execute(String startDate, String endDate) {

		log.info("解析日志数据范围开始：" + startDate + ",结束：" + endDate);
		Set<String> dateSet = DateUtil.getBetweenDates(startDate, endDate, true);
		long startTime = System.currentTimeMillis();
		long recordsCount = 0;
		String jobProcessDetail = "成功解析日志文件并保存到Hive表";
		String jobProcessCode = Constant.ETL_SUCCESS_FLAG;
		String result = "";

		try {
			for (String dateStr : dateSet) {
				log.info("开始解析：" + dateStr);
				recordsCount+=executeJob(dateStr);
			}
		} catch (Exception e) {
			log.error("解析日志文件失败！", e);
			jobProcessDetail = "异常信息为：" + e.toString();
			jobProcessCode = Constant.ETL_ERROR_FLAG;
			result = jobProcessDetail;
		}
		insertLog(startTime, jobProcessDetail, jobProcessCode, new Long(recordsCount).intValue());
		log.info("日志记录结束！");

		HiveConnection.getImpalaUtils().refreshImpalaTable(
				ParseNginxLogParameter.TARGET_DB_NAME + ParseNginxLogParameter.TARGET_PAGE_EVENT_TABLE_NAME);
		HiveConnection.getImpalaUtils().refreshImpalaTable(
				ParseNginxLogParameter.TARGET_DB_NAME + ParseNginxLogParameter.TARGET_PAGE_DATA_TABLE_NAME);
	}

	public long executeJob(String dateStr) throws Exception {
		
		long count=0;

		try {
			
			String inputPath = getOrigFilePath(dateStr);
			String errorLinePath = inputPath.split("\\.")[0] + "_" + "error";
			log.info(errorLinePath);
			
			removeHistoryFile(errorLinePath);
			
			String outPath = outputPath + "out/";
			String hdfsPre = fileSystem.getName();
			String pageEventDesFilePath = hdfsPre + outputPath
					+ ParseNginxLogParameter.PARSE_PAGE_EVENT_RESULT_FILENAME;
			String pageDataDesFilePath = hdfsPre + outputPath + ParseNginxLogParameter.PARSE_PAGE_DATA_RESULT_FILENAME;
			

			System.out.println("inputPath-->" + inputPath);
			System.out.println("outPath-->" + outPath);
			System.out.println("pageEventDesFilePath-->" + pageEventDesFilePath);
			System.out.println("pageDataDesFilePath-->" + pageDataDesFilePath);
			System.out.println("errorLinePath-->" + errorLinePath);

			Configuration hadoopConfig = new Configuration();
			hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
//			hadoopConfig.addResource("core-site.xml");
			hadoopConfig.set("dateStr", dateStr);
			hadoopConfig.set("errorLinePath", errorLinePath);
			hadoopConfig.set("pageEventDesFilePath", pageEventDesFilePath);
			hadoopConfig.set("pageDataDesFilePath", pageDataDesFilePath);
			@SuppressWarnings("deprecation")
			Job job = new Job(hadoopConfig);
			// 如果需要打成jar运行，需要下面这句
			job.setJarByClass(ParseWebh5LogMR.class);
			// job执行作业时输入和输出文件的路径
			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outPath));
			// 指定自定义的Mapper和Reducer作为两个阶段的任务处k理类
			job.setMapperClass(ParseWebh5LogMapper.class);
			job.setReducerClass(ParseWebh5LogReducer.class);
			// 设置最后输出结果的Key和Value的类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setJobName("ParseWebh5LogMR");

			// 设置输出文件类型
			MultipleOutputs.addNamedOutput(job, "webh5", TextOutputFormat.class, NullWritable.class, Text.class);
			// 执行job，直到完成
			job.waitForCompletion(true);
			System.out.println("Finished");
			
			Counters counters =job.getCounters();
			//获取系统内置的counter信息
			log.info("源日志文件共包含：" + counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue() + " 行");
			// 获取自定义counter信息
			count= counters.findCounter("webh5", "PAGE_EVENT_COUNT").getValue();
			log.info("PAGE_EVENT的数量为：" + count + " 行");
			log.info("PAGE_DATA的数量为：" + counters.findCounter("webh5", "PAGE_DATA_COUNT").getValue() + " 行");
			log.info("游离的数据数量：" + counters.findCounter("webh5", "NO_USE_COUNT").getValue() + " 行");
			
			loadToHive(dateStr);
			
		} catch (Exception e) {
			throw new Exception(e);
		}
		
		return count;

	}
	
	/**
	 * 删除上次程序运行时生成的历史文件
	 * @param errorLinePath
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	private void removeHistoryFile(String errorLinePath) throws FileNotFoundException, IllegalArgumentException, IOException{
		
		if (HadoopFileUtil.checkFileExist(fileSystem, errorLinePath)) {
			log.info("执行day_error文件删除！");
			HadoopFileUtil.delete(fileSystem, errorLinePath, true);
		}
		log.info(outputPath + "out");
		if (HadoopFileUtil.checkFileExist(fileSystem, outputPath + "out")) {
			log.info("执行out文件删除！");
			HadoopFileUtil.delete(fileSystem, outputPath + "out", true);
		}
		// 输出路径，必须是不存在的，空文件加也不行。
		if (HadoopFileUtil.checkFileExist(fileSystem,
				outputPath + ParseNginxLogParameter.PARSE_PAGE_EVENT_RESULT_FILENAME)) {
			log.info("执行page_event文件删除！");
			// HadoopFileUtil.delete(fileSystem,outputPath+ParseNginxLogParameter.PARSE_PAGE_EVENT_RESULT_FILENAME+"/",true);
			FileStatus[] eventFile = fileSystem
					.listStatus(new Path(outputPath + ParseNginxLogParameter.PARSE_PAGE_EVENT_RESULT_FILENAME));
			for (FileStatus fs : eventFile) {
				HadoopFileUtil.delete(fileSystem, fs.getPath().toString(), false);
			}

		}
		if (HadoopFileUtil.checkFileExist(fileSystem,
				outputPath + ParseNginxLogParameter.PARSE_PAGE_DATA_RESULT_FILENAME)) {
			log.info("执行page_data文件删除！");
			FileStatus[] dataFile = fileSystem
					.listStatus(new Path(outputPath + ParseNginxLogParameter.PARSE_PAGE_DATA_RESULT_FILENAME));
			for (FileStatus fs : dataFile) {
				HadoopFileUtil.delete(fileSystem, fs.getPath().toString(), false);
			}
		}
		
	}

	/**
	 * 将日志写入log记录表
	 */
	private void insertLog(long startTime, String jobProcessDetail, String jobProcessCode, int jobProcessRecordsCount) {
		try {
			// 得到任务执行时间
			int jobProcessTime = (int) ((System.currentTimeMillis() - startTime) / 1000);
			int iCreateTime = (int) (System.currentTimeMillis() / 1000);
			String iStyle = Constant.JOB_STYLE_MANUAL;

			String rate = Constant.JOB_FLOAT_RATE_DEFAULT;
			MysqlUtils jUtil = MysqlUtils.getMysqlUtilInstance(props.getProperty("mysql_default_db"));
			if (!isHistory) {
				iStyle = Constant.JOB_STYLE_AUTO;
				rate = MathUtil.computePercent(jUtil.getAutoJobRecordsCountForDate(jobName, DateUtils.getYesterday()),
						jobProcessRecordsCount);
			}
			if (jobProcessDetail != null && jobProcessDetail.length() > 500) {
				jobProcessDetail = jobProcessDetail.substring(0, 400);
			}
			DHQuartzJobResult result = new DHQuartzJobResult(jobName, jobProcessTime, jobProcessRecordsCount,
					jobProcessDetail, jobProcessCode, iCreateTime, iStyle, jobName, rate);
			try {
				QuartzjobResultMapper.getInstance().insertDHQuartzJobResult(result);
				log.info("job执行成功，写入成功状态成功！");
			} catch (Exception e) {
				log.info("job执行成功，写入成功状态失败！kafka发送写状态");
				Gson gson = new Gson();
				String value = gson.toJson(result);
				log.info(value);
				KafkaUtil kafkaUtil = KafkaUtil.getInstance();
				kafkaUtil.sendSync(ResourceUtil.getValue("kafka", Constant.QUARTZJOB_TOPIC), value);

				kafkaUtil.close();
			}
		} catch (SQLException e) {
			log.error("插入xxx_quartzjob_result表失败！", e);
		}
	}

	/**
	 * 将临时文件写入最终表
	 * 
	 * @param strData
	 */
	private void loadToHive(String date) {

		final String[] partitions = new String[] { parseNginxLogUtils.getPartitionByDate(date) };
		// 表赋值需要的sql(不包含源数据库和源表)
		final String sql_pre = ParseNginxLogParameter.COPY_SOURCE_TO_TARGET_SQL_PRE;
		// page_event表的load和copy及源表数据删除
		hiveUtils = HiveConnection.getHiveUtils();
		ParseWebh5LogMapper pm = new ParseWebh5LogMapper();
		try {
			String ePath = props.getProperty("mysqldata_path")
					+ ParseNginxLogParameter.PARSE_PAGE_EVENT_RESULT_FILENAME;
			FileStatus[] eventFile = fileSystem.listStatus(new Path(ePath));

			String pePath = ePath + "/";
			String pemergePath = mergeTargetHdfsFile(Arrays.asList(eventFile), pePath);

			System.out.println("pePath:" + pePath);
			System.out.println("pemergePath:" + pemergePath);
			// load
			final String sourcePageEventTableName = ParseNginxLogParameter.SOURCE_DB_NAME
					+ ParseNginxLogParameter.SOURCE_PAGE_EVENT_TABLE_NAME;
			if (StringUtils.isNotEmpty(pemergePath)) {
				hiveUtils.loadHdfsFileIntoTable(pemergePath, sourcePageEventTableName, true);
				// copy
				String targetPageEventTableName = ParseNginxLogParameter.TARGET_DB_NAME
						+ ParseNginxLogParameter.TARGET_PAGE_EVENT_TABLE_NAME;
				String sql = sql_pre + sourcePageEventTableName;
				hiveUtils.insertHiveDataByPartitioned(targetPageEventTableName, sql, true, partitions);
				// delete
				hiveUtils.truncateTable(sourcePageEventTableName);
			}

		} catch (Exception e) {
			throw new RuntimeException("导入失败：", e);
		}
		// page_data表的load和copy及源表数据删除
		try {

			String dPath = props.getProperty("mysqldata_path") + ParseNginxLogParameter.PARSE_PAGE_DATA_RESULT_FILENAME;
			FileStatus[] dataFile = fileSystem.listStatus(new Path(dPath));
			String pdPath = dPath + "/";
			String pdmergePath = mergeTargetHdfsFile(Arrays.asList(dataFile), pdPath);
			System.out.println("pdPath:" + pdPath);
			System.out.println("pdmergePath:" + pdmergePath);
			// load
			final String sourcePageDataTableName = ParseNginxLogParameter.SOURCE_DB_NAME
					+ ParseNginxLogParameter.SOURCE_PAGE_DATA_TABLE_NAME;
			if (StringUtils.isNotEmpty(pdmergePath)) {
				hiveUtils.loadHdfsFileIntoTable(pdmergePath, sourcePageDataTableName, true);
				// copy
				String targetPageDataTableName = ParseNginxLogParameter.TARGET_DB_NAME
						+ ParseNginxLogParameter.TARGET_PAGE_DATA_TABLE_NAME;
				String sql = sql_pre + sourcePageDataTableName;
				hiveUtils.insertHiveDataByPartitioned(targetPageDataTableName, sql, true, partitions);
				// delete
				hiveUtils.truncateTable(sourcePageDataTableName);
			}

		} catch (Exception e) {
			throw new RuntimeException("导入失败：", e);
		}
	}

	/**
	 * 合并多个文件返回合并后文件名
	 * 
	 * @param fileStatus
	 * @return
	 * @throws Exception
	 */
	private String mergeTargetHdfsFile(List<FileStatus> fileStatus, String logPath) throws Exception {

		if (fileStatus.size() == 0) {
			return null;
		} else if (fileStatus.size() == 1) {
			return logPath + fileStatus.get(0).getPath().getName();
		}
		// 合并之后的文件名
		String mergePath = logPath + "target_webh5.log";
		Path mergeFile = new Path(mergePath);
		// 只执行一次合并
		if (!fileSystem.exists(mergeFile)) {
			log.info("存在" + fileStatus.size() + "个文件,开始合并文件！");
			// 创建输出流
			FSDataOutputStream out = fileSystem.create(mergeFile);
			for (FileStatus status : fileStatus) {
				Path temp = status.getPath();
				FSDataInputStream in = fileSystem.open(temp);
				IOUtils.copyBytes(in, out, 4096, false);
				in.close();
			}
			out.close();
			log.info("文件合并完成：" + mergePath);
		}
		return mergePath;
	}

	/**
	 * 获取源日志路径
	 * 
	 * @param date
	 * @return
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	private String getOrigFilePath(String date) throws Exception {
		String hdfsLogPathPre = ParseNginxLogParameter.HDFS_LOG_PATH_PRE;
		String dateStr = date.replace("-", "") + Constant.SLASH;
		String removeFlumeLogPathPre = StringUtils.replace(hdfsLogPathPre, ParseNginxLogParameter.FOLDER_FLUME, "");
		String origFilePath = removeFlumeLogPathPre + parseNginxLogUtils.getFilePathByDate(dateStr);
		log.info("备用查询的日志路径：" + origFilePath);
		Path folder = new Path(hdfsLogPathPre + dateStr);
		if (fileSystem.exists(folder)) {
			FileStatus[] fileStatus = fileSystem.listStatus(folder);
			// 找出所有以tongji.开头的文件
			List<FileStatus> tongjiFile = new ArrayList<FileStatus>();
			for (FileStatus status : fileStatus) {
				if (status.isFile()) {
					String filePre = StringUtils.substring(status.getPath().getName(), 0, 7);
					if (ParseNginxLogParameter.FLUME_FILE_PRE.equals(filePre)) {
						tongjiFile.add(status);
					}
				}
			}
			String mergePath = mergeHdfsFile(tongjiFile, dateStr);
			if (mergePath != null) {
				origFilePath = mergePath;
			}
		}
		return origFilePath;
	}

	/**
	 * 获取fileSystem
	 * 
	 * @return
	 */
	private FileSystem getFileSystem() {
		if (fileSystem == null) {
			String[] hosts = props.getProperty(Config.hdfs_host).split(Constant.DELIMITER_SEMICOLON);
			fileSystem = HadoopFileUtil.getFileSystem(hosts);
		}
		return fileSystem;
	}

	/**
	 * 合并多个文件返回合并后文件名
	 * 
	 * @param fileStatus
	 * @return
	 * @throws Exception
	 */
	private String mergeHdfsFile(List<FileStatus> fileStatus, String dateStr) throws Exception {
		String hdfsLogPathPre = ParseNginxLogParameter.HDFS_LOG_PATH_PRE;
		if (fileStatus.size() == 0) {
			return null;
		} else if (fileStatus.size() == 1) {
			return hdfsLogPathPre + dateStr + fileStatus.get(0).getPath().getName();
		}
		// 合并之后的文件名
		String mergePath = hdfsLogPathPre + dateStr + ParseNginxLogParameter.TONGJI_MERGE_FILENAME;
		Path mergeFile = new Path(mergePath);
		// 只执行一次合并
		if (!fileSystem.exists(mergeFile)) {
			log.info("存在" + fileStatus.size() + "个文件,开始合并文件！");
			// 创建输出流
			FSDataOutputStream out = fileSystem.create(mergeFile);
			for (FileStatus status : fileStatus) {
				Path temp = status.getPath();
				FSDataInputStream in = fileSystem.open(temp);
				IOUtils.copyBytes(in, out, 4096, false);
				in.close();
			}
			out.close();
			log.info("文件合并完成：" + mergePath);
		}
		return mergePath;
	}

}
