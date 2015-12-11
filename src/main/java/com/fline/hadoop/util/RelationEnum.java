package com.fline.hadoop.util;

/**
 * solr ��ѯ��ϵ
 * @author hetao
 *
 */
public enum RelationEnum {

	AND{
		@Override public String getCode(){return "AND";}
		@Override public String getName(){return "����";}
	},
	OR{
		@Override public String getCode(){return "OR";}
		@Override public String getName(){return "��";}
	},
	LIKE{
		@Override public String getCode(){return "*";}
		@Override public String getName(){return "ģ��";}
	};
	public abstract String getCode();
	public abstract String getName();
}
