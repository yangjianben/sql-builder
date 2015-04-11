package com.github.jonathanhds.sqlbuilder.builder;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.github.jonathanhds.sqlbuilder.Context;
import com.github.jonathanhds.sqlbuilder.Database;
import com.github.jonathanhds.sqlbuilder.delete.Delete;
import com.github.jonathanhds.sqlbuilder.insert.Insert;
import com.github.jonathanhds.sqlbuilder.select.Select;
import com.github.jonathanhds.sqlbuilder.update.Update;

public class QueryBuilder {

	private final Context context;

	public QueryBuilder(Database database, DataSource dataSource) throws SQLException {
		this(database, dataSource.getConnection());
	}

	public QueryBuilder(Database database, Connection connection) {
		this.context = new Context(database, connection);
	}

	public Select select() {
		return new Select(context);
	}

	public Update update() {
		return new Update(context);
	}

	public Update update(String table) {
		return new Update(context, table);
	}

	public Delete delete() {
		return new Delete(context);
	}

	public Delete delete(String table) {
		return new Delete(context, table);
	}

	public Insert insert() {
		return new Insert(context);
	}

	public Insert insert(String table) {
		return new Insert(context, table);
	}

	@Override
	public String toString() {
		return context.toString();
	}

	/**
	 * 
	 * @return Context
	 * @Author 杨健/YangJian
	 * @Date 2015年4月11日 下午2:39:40
	 * @Version 1.0.0
	 */
	public Context getContext() {
		return this.context;
	}
}