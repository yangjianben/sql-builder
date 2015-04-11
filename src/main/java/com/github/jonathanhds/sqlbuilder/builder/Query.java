package com.github.jonathanhds.sqlbuilder.builder;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.github.jonathanhds.sqlbuilder.select.AndCondition;
import com.github.jonathanhds.sqlbuilder.select.CountRowMapper;

/**
 * Query基类<br>
 * 
 * @describe：封装 CriteriaBuilder查询条件
 * @author：yangjian1004
 * @since：2011-11-23
 * @since：2015-04-09
 */
@SuppressWarnings({ "unused", "unchecked", "rawtypes" })
public class Query implements Serializable {

	private static final long serialVersionUID = 1L;

	/** 要查询的模型对象 */
	private Class clazz;
	private Connection connection;
	private QueryBuilder queryBuilder;
	private AndCondition andCondition;
	private String table;

	public Class getClazz() {
		return this.clazz;
	}

	private Query() {
	}

	private Query(String table, Connection connection) {
		this.table = table;
		this.connection = connection;
		this.queryBuilder = new QueryBuilder(null, connection);
		this.andCondition = new AndCondition(this.queryBuilder.getContext());

	}

	public Query(String table, DataSource dataSource) throws SQLException {
		this.table = table;
		this.connection = dataSource.getConnection();
		this.queryBuilder = new QueryBuilder(null, connection);
		this.andCondition = new AndCondition(this.queryBuilder.getContext());
	}

	/** 通过类创建查询条件 */
	public static Query forClass(String table, Connection connection) {
		return new Query(table, connection);
	}

	/**
	 * 通过类创建查询条件
	 * 
	 * @throws SQLException
	 */
	public static Query forClass(String table, DataSource dataSource) throws SQLException {
		return new Query(table, dataSource);
	}

	/** 增加子查询 */
	private void addSubQuery(String propertyName, Query query) {
	}

	private void addSubQuery(Query query) {

	}

	/** 增关联查询 */
	public void addLinkQuery(String propertyName, Query query) {

	}

	/** 相等 */
	public void eq(String propertyName, Object value) {
		if (isEmpty(value))
			return;
		this.andCondition.add(propertyName, value);
	}

	public void or(List<String> propertyName, Object value) {
		if (isEmpty(value))
			return;
		if ((propertyName == null) || (propertyName.size() == 0))
			return;

	}

	/** 空 */
	public void isNull(String propertyName) {

	}

	/** 非空 */
	public void isNotNull(String propertyName) {

	}

	/** 不相等 */
	public void notEq(String propertyName, Object value) {
		if (isEmpty(value)) {
			return;
		}

	}

	/**
	 * not in
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            值集合
	 */
	public void notIn(String propertyName, Collection value) {
		if ((value == null) || (value.size() == 0)) {
			return;
		}
	}

	/**
	 * 模糊匹配
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            属性值
	 */
	public void like(String propertyName, String value) {
		if (isEmpty(value))
			return;
		if (value.indexOf("%") < 0)
			value = "%" + value + "%";
	}

	/**
	 * 时间区间查询
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param lo
	 *            属性起始值
	 * @param go
	 *            属性结束值
	 */
	public void between(String propertyName, Date lo, Date go) {
		if (isNotEmpty(lo) && isNotEmpty(go)) {
			return;
		}

		if (isNotEmpty(lo) && isEmpty(go)) {
			return;
		}

		if (isNotEmpty(go)) {
		}

	}

	public void between(String propertyName, Number lo, Number go) {
		if (!(isEmpty(lo)))
			ge(propertyName, lo);

		if (!(isEmpty(go)))
			le(propertyName, go);
	}

	/**
	 * 小于等于
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            属性值
	 */
	public void le(String propertyName, Number value) {
		if (isEmpty(value)) {
			return;
		}
	}

	/**
	 * 小于
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            属性值
	 */
	public void lt(String propertyName, Number value) {
		if (isEmpty(value)) {
			return;
		}
	}

	/**
	 * 大于等于
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            属性值
	 */
	public void ge(String propertyName, Number value) {
		if (isEmpty(value)) {
			return;
		}
	}

	/**
	 * 大于
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            属性值
	 */
	public void gt(String propertyName, Number value) {
		if (isEmpty(value)) {
			return;
		}
	}

	/**
	 * in
	 * 
	 * @param propertyName
	 *            属性名称
	 * @param value
	 *            值集合
	 */
	public void in(String propertyName, Collection value) {
		if ((value == null) || (value.size() == 0)) {
			return;
		}
	}

	/**
	 * 创建查询条件
	 * 
	 * @return 离线查询
	 */
	public String newCriteriaQuery() {
		return null;
	}

	private void addLinkCondition(Query query) {

	}

	public void addOrder(String propertyName, String order) {
		if (order == null || propertyName == null)
			return;

	}

	public void setFetchModes(List<String> fetchField, List<String> fetchMode) {

	}

	// ====================================================
	// ======================= 查询操作 =======================
	// ====================================================
	/** 每次批量操作数 */
	private int batchSize = 50;

	/** 设置每次操作数 */
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public <E> E get(Class clazz, Serializable id) {
		return null;
	}

	/**
	 * 统计记录
	 * 
	 * @param query
	 *            统计条件
	 */
	public Integer getCount() {
		Integer count = null;
		try {
			count = this.queryBuilder.select().count("*").from().table(getTableName()).where()
					.single(new CountRowMapper());
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return count;
	}

	private String getTableName() {
		return table;
	}

	/**
	 * 根据query查找记录
	 * 
	 * @param query
	 *            查询条件
	 * @param firstResult
	 *            起始行
	 * @param maxResults
	 *            结束行
	 */
	public <E extends Serializable> List<E> query(int firstResult, int maxResults) {
		List result = null;
		return result;
	}

	/**
	 * 根据query查找记录
	 * 
	 * @param query
	 *            查询条件
	 */
	public <E extends Serializable> List<E> query() {
		return null;
	}

	/**
	 * 分页查询
	 * 
	 * @param query
	 *            查询条件
	 * @param pageNo
	 *            页号
	 * @param rowsPerPage
	 *            每页显示条数
	 */
	public Map<String, Object> queryPage(int pageNo, int rowsPerPage) {
		int count = getCount().intValue();
		int firstResult = calc(pageNo, rowsPerPage, count);
		List result = null;
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("count", count);
		resultMap.put("pageNo", pageNo);
		resultMap.put("rowsPerPage", rowsPerPage);
		resultMap.put("result", result);
		return resultMap;
	}

	/**
	 * 计算起始条数
	 */
	public int calc(int pageNo, int rowsPerPage, int count) {
		if (pageNo <= 0)
			pageNo = 1;
		if (rowsPerPage <= 0)
			rowsPerPage = 15;

		// 当把最后一页数据删除以后,页码会停留在最后一个上必须减一
		int totalPageCount = count / rowsPerPage;
		if (pageNo > totalPageCount && (count % rowsPerPage == 0)) {
			pageNo = totalPageCount;
		}
		if (pageNo - totalPageCount > 2) {
			pageNo = totalPageCount + 1;
		}
		int firstRow = (pageNo - 1) * rowsPerPage;
		if (firstRow < 0) {
			firstRow = 0;
		}
		return firstRow;
	}

	/**
	 * 插入记录
	 * 
	 * @param entity
	 *            要插入的记录
	 */
	public void insert(Object entity) {
		if (entity instanceof List) {
			insertList((List) entity);
			return;
		} else if (entity instanceof Object[]) {
			return;
		}
		try {
			// entityManager.persist(entity);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 批量增加
	 * 
	 * @param list
	 *            要新增的数据
	 */
	public void insertList(List list) {
		if (list == null || list.size() == 0) {
			return;
		}
		int i = 0;
		for (Object o : list) {
			insert(o);
			if (i % batchSize == 0) {
				// entityManager.flush();
			}
			i++;
		}
	}

	/**
	 * 更新记录
	 * 
	 * @param entity
	 *            要更新的记录
	 */
	public void update(Object entity) {
		if (entity instanceof List) {
			this.updateList((List) entity);
			return;
		}
		// entityManager.merge(entity);
	}

	/**
	 * 更新entityManager
	 */
	public void updateList(List list) {
		for (Object entity : list) {
			this.update(entity);
		}
	}

	/**
	 * 删除记录
	 * 
	 * @param entity
	 *            要删除的记录
	 */
	public void delete(Object entity) {
		if (entity instanceof List) {
			List list = (List) entity;
			for (Object o : list) {
				// entityManager.remove(o);
			}
		} else {
			// entityManager.remove(entity);
		}
	}

	/**
	 * 根据ids删除数据
	 * 
	 * @param entity
	 *            删除实体类
	 * @param ids
	 *            删除条件
	 */
	public void delete(Class entity, List ids) {
		String idName = getPrimaryKeyName();
		StringBuffer sb = new StringBuffer();
		sb.append(idName + " in(");
		for (int i = 0; i < ids.size(); i++) {
			sb.append("'" + ids.get(i) + "',");
		}
		String jpqlCondition = sb.substring(0, sb.length() - 1) + ")";
		delete(entity, jpqlCondition);
	}

	public void delete(Class entity, String jpqlCondition) {
		if (isEmpty(jpqlCondition)) {
			jpqlCondition = "1=1";
		}
		int no = updateJpql("delete " + entity.getName() + " where " + jpqlCondition);
	}

	public <E extends Serializable> List<E> query(String jpql, int firstResult, int maxResults) {
		List result = null;
		return result;
	}

	public <E extends Serializable> List<E> queryBySql(String sql, int firstResult, int maxResults) {
		return null;
	}

	public <E extends Serializable> List<E> queryAll(Class clazz) {
		return null;
	}

	public <E extends Serializable> List<E> query(String jpql) {
		return null;
	}

	public Integer updateJpql(String jpql) {
		return null;
	}

	public Integer updateSql(String sql) {
		return null;
	}

	public <E extends Serializable> List<E> queryBySql(String sql) {
		return null;
	}

	/**
	 * 获得主键名称
	 * 
	 * @param clazz
	 *            操作是实体对象
	 * @param EntityManager
	 *            entityManager工厂
	 * @return 初建名称
	 * */
	public String getPrimaryKeyName() {
		return null;
	}

	/**
	 * 判断一个对象是否为空。它支持如下对象类型：
	 * <ul>
	 * <li>null : 一定为空
	 * <li>字符串 : ""为空,多个空格也为空
	 * <li>数组
	 * <li>集合
	 * <li>Map
	 * <li>其他对象 : 一定不为空
	 * </ul>
	 * 
	 * @param obj
	 *            任意对象
	 * @return 是否为空
	 */
	public final static boolean isEmpty(final Object obj) {
		if (obj == null) {
			return true;
		}
		if (obj instanceof String) {
			return "".equals(String.valueOf(obj).trim());
		}
		if (obj.getClass().isArray()) {
			return Array.getLength(obj) == 0;
		}
		if (obj instanceof Collection<?>) {
			return ((Collection<?>) obj).isEmpty();
		}
		if (obj instanceof Map<?, ?>) {
			return ((Map<?, ?>) obj).isEmpty();
		}
		return false;
	}

	public final static boolean isNotEmpty(final Object obj) {
		return !isEmpty(obj);
	}
}
