data-client-api 说明书v0.1.0
============================
@2015.12.11

- [概述](#description)
- [主要类说明](#mainClass)
	- [HbaseOperator](#HbaseOperator)
- [主要接口调用](#mainInterface)
	- [RDB数据迁移到HBASE](#DataImportFromRDB2HBASE)
	- [Hbase根据时间范围查数据](#HbaseQueryByTime)
		- 返回rowkey集合
	- [Hbase内容查询](#HbaseQueryByContent)
		- 返回`List<Object>`
	- [Hbase添加/更新数据](#HbaseUpdateData)
	- [Hbase删除数据](#HbaseDeleteData)
- [实例介绍](#samples)
- [SDK使用说明](#sdkUseStep)

##<span id="description">概述</span>
Fline_Hadoop SDK v0.1.0用于描述多种数据迁移功能的实现和使用。当前client api支持的功能有：从关系型数据导出到HBASE，基于http请求的HBASE增、删、改、查操作。

##<span id="mainClass">主要类说明</span>


##<span id="mainInterface">主要接口调用</span>


##<span id="samples">实例介绍</span>


##<span id="sdkUseStep">SDK使用说明</span>

###1. 在項目中添加fline-0.1.0.jar及其依賴包
	主要类说明：com.fline.hadoop.hbase.HbaseOperator hbase相关操作类.
	该类主要用于从关系型数据导出到HBASE，并且提供l HBASE的查询、管理等功能
