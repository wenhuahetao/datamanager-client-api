package com.fline.hadoop.util;

/**
 * solr 查询关系
 * @author hetao
 *
 */
public enum RelationEnum {

	AND{
		@Override public String getCode(){return "AND";}
		@Override public String getName(){return "并列";}
	},
	OR{
		@Override public String getCode(){return "OR";}
		@Override public String getName(){return "或";}
	},
	LIKE{
		@Override public String getCode(){return "*";}
		@Override public String getName(){return "模糊";}
	};
	public abstract String getCode();
	public abstract String getName();
}
