MR对日志进行解析操作


所需依赖文件配置

<!-- HADOOP 依赖 -->
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-client</artifactId>
			<version>2.6.0</version>
			<exclusions>
				<exclusion>
					<groupId>tomcat</groupId>
					<artifactId>jasper-compiler</artifactId>
				</exclusion>
				<exclusion>
					<groupId>tomcat</groupId>
					<artifactId>jasper-runtime</artifactId>
				</exclusion>
			</exclusions>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>2.6.0</version>
			<exclusions>
				<exclusion>
					<groupId>tomcat</groupId>
					<artifactId>jasper-compiler</artifactId>
				</exclusion>
				<exclusion>
					<groupId>tomcat</groupId>
					<artifactId>jasper-runtime</artifactId>
				</exclusion>
			</exclusions>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-hdfs</artifactId>
			<version>2.6.0</version>
		</dependency>
		
		
单机命令：
   java -cp exec.jar com.cn.ParseWebh5LogMR  2008-01-01  
   
集群命令:
   hadoop jar exec.jar com.cn.ParseWebh5LogMR 2008-01-01
   
常见问题:
   
   执行过程记录不同类型数据统计，自定义变量，进行++操作，在单机情况下可以正常统计，在集群下，无法得到预计结果，得到的为初始值 (由于进群特性)。
   
   解决方案：
   
     利用MR计时器进行统计(需在job完成后进行统计操作，否则异常)
     
     在map或reduce端进行统计set操作：
     
     context.getCounter("webh5", "PAGE_EVENT_COUNT").increment(1);
     
     在主方法进行结果获取操作：
     
     Counters counters =job.getCounters();    
     // 获取系统内置的counter信息
     counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue()
     // 获取自定义counter信息
     count= counters.findCounter("webh5", "PAGE_EVENT_COUNT").getValue();
    

