package com.github.jonathanhds.sqlbuilder.select;

import com.github.jonathanhds.sqlbuilder.Context;

public class AndCondition extends Condition {

	public AndCondition(Context context) {
		super(context);
	}

	@Override
	public String getPrefix() {
		return "AND";
	}

}