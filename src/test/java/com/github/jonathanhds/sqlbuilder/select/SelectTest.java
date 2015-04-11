package com.github.jonathanhds.sqlbuilder.select;

import static com.github.jonathanhds.sqlbuilder.select.OrderByType.DESC;
import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.HOUR_OF_DAY;
import static java.util.Calendar.MILLISECOND;
import static java.util.Calendar.MINUTE;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.SECOND;
import static java.util.Calendar.YEAR;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.jonathanhds.sqlbuilder.builder.Query;
import com.github.jonathanhds.sqlbuilder.builder.QueryBuilderHSQLDB;
import com.github.jonathanhds.sqlbuilder.builder.QueryBuilderOracle;
import com.github.jonathanhds.sqlbuilder.support.Country;
import com.github.jonathanhds.sqlbuilder.support.CountryRowMapper;
import com.github.jonathanhds.sqlbuilder.support.MemoryDatabase;
import com.github.jonathanhds.sqlbuilder.support.Person;
import com.github.jonathanhds.sqlbuilder.support.PersonRowMapper;

public class SelectTest {

	private Connection connection;

	@Before
	public void startConnection() throws Exception {
		connection = new MemoryDatabase().getConnection();
		connection.createStatement().executeQuery("SET DATABASE SQL SYNTAX ORA FALSE");
	}

	@After
	public void closeConnection() throws Exception {
		connection.createStatement().execute("SHUTDOWN");

		if (!connection.isClosed()) {
			connection.close();
		}
	}

	@Test
	public void selectAllFromTable() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p")
				.list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(
				persons,
				containsInAnyOrder(jonathan().setCountry(null), steveJobs().setCountry(null),
						domPedro().setCountry(null)));
	}

	@Test
	public void selectAllFromTableWhereConditions() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p").where()
				.and("p.name = 'Jonathan'").list(new PersonRowMapper());

		assertThat(persons, hasSize(1));
		assertThat(persons, containsInAnyOrder(jonathan().setCountry(null)));

		persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p")
				.where("p.name = 'Jonathan'").list(new PersonRowMapper());

		assertThat(persons, hasSize(1));
		assertThat(persons, containsInAnyOrder(jonathan().setCountry(null)));
	}

	@Test
	public void selectAllFromTableWhereConditionWithParameter() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p").where()
				.and("p.name = ?", "Steve Jobs").and("p.birthday = ?", getNullValue()).list(new PersonRowMapper());

		assertThat(persons, hasSize(1));
		assertThat(persons, containsInAnyOrder(steveJobs().setCountry(null)));
	}

	@Test
	public void selectAllFromTableWhereBetween() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p")
				.innerJoin("COUNTRY c ON c.id = p.country_id").where()
				.andBetween("p.birthday", toDate(1988, 1, 1), getNullValue()).list(new PersonRowMapper());

		assertThat(persons, hasSize(1));
		assertThat(persons, containsInAnyOrder(jonathan()));
	}

	@Test
	public void selectColumnsFromTableJoinAnotherTable() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().column("p.name").column("p.birthday")
				.column("p.country_id").column("c.country_name").from().table("PERSON p")
				.innerJoin("COUNTRY c ON c.id = p.country_id").list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(persons, containsInAnyOrder(jonathan(), steveJobs(), domPedro()));

		persons = new QueryBuilderHSQLDB(connection).select()
				.columns("p.name", "p.birthday", "p.country_id", "c.country_name").from().table("PERSON p")
				.innerJoin("COUNTRY c ON c.id = p.country_id").list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(persons, containsInAnyOrder(jonathan(), steveJobs(), domPedro()));

		persons = new QueryBuilderHSQLDB(connection).select()
				.columns("p.name", "p.birthday", "p.country_id", "c.country_name").from().table("PERSON p")
				.table("country c").where("c.id = p.country_id").list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(persons, containsInAnyOrder(jonathan(), steveJobs(), domPedro()));

		persons = new QueryBuilderHSQLDB(connection).select()
				.columns("p.name", "p.birthday", "p.country_id", "c.country_name").from()
				.tables("PERSON p", "COUNTRY c").where("c.id = p.country_id").list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(persons, containsInAnyOrder(jonathan(), steveJobs(), domPedro()));

		persons = new QueryBuilderHSQLDB(connection).select()
				.columns("p.name", "p.birthday", "p.country_id", "c.country_name").from().table("PERSON p")
				.select("select * from country", "c").where("c.id = p.country_id").list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(persons, containsInAnyOrder(jonathan(), steveJobs(), domPedro()));
	}

	@Test
	public void selectAllFromTableOrderBy() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p").orderBy()
				.column("p.name", OrderByType.DESC).list(new PersonRowMapper());

		assertThat(persons, hasSize(3));
		assertThat(persons,
				contains(steveJobs().setCountry(null), jonathan().setCountry(null), domPedro().setCountry(null)));
	}

	@Test
	public void selectAllFromTableSingleResult() throws Exception {
		Person person = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p").where()
				.and("p.id = ?", 1).single(new PersonRowMapper());

		assertThat(person, equalTo(jonathan().setCountry(null)));
	}

	@Test(expected = SQLException.class)
	public void selectAllFromTableSingleResult_throwExceptionIfMoreThanOneResultIsReturned() throws Exception {
		new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p").single(new PersonRowMapper());
	}

	@Test
	public void selectAllFromTableGroupBy() throws Exception {
		List<Integer> counts = new QueryBuilderHSQLDB(connection).select().count("*").from().table("PERSON p")
				.groupBy().column("p.birthday").list(new CountRowMapper());

		assertThat(counts, hasSize(3));
		assertThat(counts, contains(new Integer(1), new Integer(1), new Integer(1)));

		counts = new QueryBuilderHSQLDB(connection).select().count("*").from().table("PERSON p").groupBy("p.birthday")
				.list(new CountRowMapper());

		assertThat(counts, hasSize(3));
		assertThat(counts, contains(new Integer(1), new Integer(1), new Integer(1)));

		counts = new QueryBuilderHSQLDB(connection).select().count("c.country_name").from()
				.tables("PERSON p", "COUNTRY c").where("p.country_id = c.id").groupBy("c.country_name")
				.list(new CountRowMapper());

		assertThat(counts, hasSize(2));
		assertThat(counts, containsInAnyOrder(new Integer(2), new Integer(1)));
	}

	@Test
	public void selectCountFromTableGroupByHaving() throws Exception {
		List<Integer> counts = new QueryBuilderHSQLDB(connection).select().count("c.country_name").from()
				.tables("COUNTRY c", "PERSON p").where("c.id = p.country_id").groupBy("c.country_name")
				.having("count(c.country_name) > 1").list(new CountRowMapper());

		assertThat(counts, hasSize(1));
		assertThat(counts, contains(2));
	}

	/**
	 * 
	 * @throws Exception
	 *             void
	 * @Author 杨健/YangJian
	 * @Date 2015年4月11日 下午3:01:54
	 * @Version 1.0.0
	 */
	@Test
	public void selectCountFromTable() throws Exception {
		Query query = Query.forClass("PERSON", connection);
		query.eq("name", "Jonathan");// p.name = 'Jonathan'
		Integer count = query.getCount();
		System.out.println(count);
	}

	@Test
	public void selectColumnsFromTableGroupByHavingOrderBy() throws Exception {
		List<Country> countries = new QueryBuilderHSQLDB(connection).select().column("c.country_name").from()
				.tables("PERSON p", "COUNTRY c").where("c.id = p.country_id").groupBy("c.country_name")
				.having("count(1) > 1").orderBy("c.country_name", DESC).list(new CountryRowMapper());

		assertThat(countries, hasSize(1));
		assertThat(countries, contains(brazil()));

		countries = new QueryBuilderHSQLDB(connection).select().column("c.country_name").from()
				.tables("PERSON p", "COUNTRY c").where("c.id = p.country_id").groupBy("c.country_name")
				.orderBy("c.country_name", DESC).list(new CountryRowMapper());

		assertThat(countries, hasSize(2));
		assertThat(countries, contains(usa(), brazil()));
	}

	@Test
	public void selectAllFromTableWithPagination_Oracle() throws Exception {
		connection.createStatement().executeQuery("SET DATABASE SQL SYNTAX ORA TRUE");
		List<Person> persons = new QueryBuilderOracle(connection).select().all().from().table("PERSON p").orderBy()
				.column("p.name", OrderByType.ASC).limit(0, 2).list(new PersonRowMapper());

		assertThat(persons, hasSize(2));
		assertThat(persons, containsInAnyOrder(domPedro().setCountry(null), jonathan().setCountry(null)));
	}

	@Test
	public void selectAllFromTableWithPagination_HSQLDB() throws Exception {
		List<Person> persons = new QueryBuilderHSQLDB(connection).select().all().from().table("PERSON p").orderBy()
				.column("p.name", OrderByType.ASC).limit(0, 2).list(new PersonRowMapper());

		assertThat(persons, hasSize(2));
		assertThat(persons, contains(domPedro().setCountry(null), jonathan().setCountry(null)));
	}

	private Date toDate(int year, int month, int day) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(DAY_OF_MONTH, day);
		calendar.set(MONTH, month - 1);
		calendar.set(YEAR, year);
		calendar.set(HOUR_OF_DAY, 0);
		calendar.set(MINUTE, 0);
		calendar.set(SECOND, 0);
		calendar.set(MILLISECOND, 0);
		return calendar.getTime();
	}

	private Country brazil() {
		return new Country("Brazil");
	}

	private Country usa() {
		return new Country("United States of America");
	}

	private Object getNullValue() {
		return null;
	}

	private Person domPedro() {
		return new Person("Dom Pedro II", toDate(1825, 12, 02), brazil());
	}

	private Person steveJobs() {
		return new Person("Steve Jobs", toDate(1955, 2, 24), usa());
	}

	private Person jonathan() {
		return new Person("Jonathan", toDate(1988, 11, 8), brazil());
	}
}