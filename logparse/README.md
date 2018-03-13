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

   java -cp target.jar com.cn.ParseWebh5LogMR  2008-01-01                                                                                   
   
集群命令:

   hadoop jar target.jar com.cn.ParseWebh5LogMR 2008-01-01
