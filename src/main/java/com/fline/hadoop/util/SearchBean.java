package com.fline.hadoop.util;


public class SearchBean {
	
	private String name;
	
	private String value;
	
	private RelationEnum relation;

	/**
	 * @param name
	 * @param value
	 * @param realtion
	 */
	public SearchBean(String name, String value, RelationEnum relation) {
		this.name = name;
		this.relation = relation;
		this.value = value;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public RelationEnum getRelation() {
		return relation;
	}

	public void setRelation(RelationEnum relation) {
		this.relation = relation;
	}
}