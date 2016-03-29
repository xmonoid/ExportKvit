/**
 * 
 */
package ru.nlmk_it.db2file.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import oracle.jdbc.internal.OracleCallableStatement;
import oracle.jdbc.internal.OraclePreparedStatement;

import org.apache.log4j.Logger;

/**
 * Класс, в обязанность которого вменяется исполнение SQL сценария.
 * Предполагается, что сценарий состоит из двух компонентов:
 * <ol>
 *   <li>Процедура, вызывающаяся командой <code>call</code> (может отсутствовать).</li>
 *   <li>SQL-запрос, выдающий итоговую информацию.</li>
 * </ol>
 * 
 * @author kosyh_ev
 *
 */
public final class SQLExecutor {
	
	private final Logger logger = Logger.getLogger(this.getClass());
	
	/**
	 * {@code Statement}-объект, через который выполняются запросы.
	 */
	private OraclePreparedStatement cst;
	
	/**
	 * Подключение к БД.
	 */
	private final Connection connection;
	
	/**
	 * Текст с вызовом процедуры.
	 */
	private final String procedure;
	
	/**
	 * Текст с SQL-выражением выгрузки.
	 */
	private final String select;
	
	/**
	 * Обслуживающая организация.
	 */
	private String pleskgesk;
	
	/**
	 * Код базы.
	 */
	private String pdb_lesk;
	
	/**
	 * Дата, на которую выгружаются квитанции.
	 */
	private java.sql.Date pdat;
	
	/**
	 * Флаг, фильтрующий пустые квитанции.
	 */
	private String pnot_empty;
	
	/**
	 * Флаг, указывающий на использование табличного фильтра.
	 */
	private String use_filter;
	
	/**
	 * ID отдельного многоквартирного дома.
	 */
	private String mkd_id;
	
	
	/**
	 * Конструктор класса, выполняющего сценарий.
	 * @param connection Подключение к БД. Должно быть уже создано!
	 * @param procedure Текст с вызовом хранимой процедуры вида {@code call myproc(&param0, &param1)}.
	 * @param select Текст с SQL-запросом, возвращающим итоговую информацию.
	 */
	public SQLExecutor(Connection connection, String procedure, String select) {
		logger.trace("Invoke constructor SQLExecutor(Connection, StoredProcedure, Select)");
		
		assert connection != null : "The connection to database isn't created.";
		
		this.connection = connection;
		this.procedure = procedure;
		this.select = select;
		
		logger.trace("Created an object: " + this.toString());
	}
	
	
	/**
	 * Задание обслуживающей организации.
	 * В принципе, организации существуют две: ЛЭСК (LESK) и ГЭСК (GESK). Но один еблан
	 * недавно добавил ещё штук шесть, так что потом могут добавиться и ещё.
	 * @param pleskgesk Название организации латиницей в виде аббревиатуры.
	 */
	public void setPleskgesk(String pleskgesk) {
		logger.trace("Invoke setPleskgesk(" + pleskgesk + ")");
		this.pleskgesk = pleskgesk;
	}
	
	
	/**
	 * Задание района, по которому будут выгружаться квитанции.
	 * Раньше их было двадцать, недавно увеличили до 90 с хреном, так что количество
	 * и коды конкретных районов узнавайте у функциональщиков.
	 * @param pdb_lesk Код района в виде двух цифр: <code>"01", "22"</code> etc.
	 */
	public void setDbLesk(String pdb_lesk) {
		logger.trace("Invoke setDbLesk(" + pdb_lesk + ")");
		this.pdb_lesk = pdb_lesk;
	}
	
	
	/**
	 * Задание даты, на которую будут выгружаться квитанции. Определяющим является только месяц,
	 * число, в принципе, можно ставить любое, но лучше писать первое. То есть, если вы хотите выгрузить
	 * квитанции за апрель 2015 года, задавайте <code>"01.04.2015"</code>.
	 * @param pdat Дата в формате dd.MM.yyyy
	 * @throws SQLException Выбрасывается, если формат даты неверен.
	 */
	public void setPdat(String pdat) throws SQLException {
		logger.trace("Invoke setPdat(" + pdat + ")");
		try {
			this.pdat = new java.sql.Date(new SimpleDateFormat("dd.MM.yyyy").parse(pdat).getTime());
		}
		catch (ParseException e) {
			throw new SQLException(e.getMessage());
		}
	}
	
	/**
	 * Флаг, сигнализирующий о фильтрации пустых квитанций.
	 * 
	 * Вообще говоря, наличие пустых квитанций - это косяк биллинга, но так как исправить его там
	 * КРАЙНЕ тяжело, был придуман такой костыль. Логика использования флага скрыта в хранимой
	 * процедуре.
	 * @param pnot_empty Значение <code>"1"</code> сигнализирует о запрете выгрузки пустых квитанций,
	 * <code>"0"</code> значит выгружать всё.
	 */
	public void setPnotEmpty(String pnot_empty) {
		logger.trace("Invoke setPnotEmpty(" + pnot_empty + ")");
		this.pnot_empty = pnot_empty;
	}
	
	/**
	 * 
	 * @param use_filter
	 */
	public void setUseFilter(String use_filter) {
		logger.trace("Invoke setUseFilter(" + use_filter + ")");
		this.use_filter = use_filter;
	}
	
	/**
	 * 
	 * @param mkd_id
	 */
	public void setMkdId(String mkd_id) {
		logger.trace("Invoke setMkdId(" + mkd_id + ")");
		this.mkd_id = mkd_id;
	}
	
	/**
	 * Выполнение процедуры.
	 * @throws SQLException Выбрасывается при возникновении ошибки в процедуре.
	 */
	public void executeProcedure() throws SQLException {
		logger.trace("Invoke executeProcedure()");
		
		cst = (OracleCallableStatement) connection.prepareCall(procedure);
//		cst.setQueryTimeout(60 * 60); // This is one hour.
		if (procedure.contains(":pleskgesk")) {
			cst.setFixedCHARAtName("pleskgesk", pleskgesk);
		}
		if (procedure.contains(":pdb_lesk")) {
			cst.setFixedCHARAtName("pdb_lesk", pdb_lesk);
		}
		if (procedure.contains(":pdat")) {
			cst.setDateAtName("pdat", pdat);
		}
		if (procedure.contains(":pnot_empty")) {
			cst.setFixedCHARAtName("pnot_empty", pnot_empty);
		}
		if (procedure.contains(":use_filter")) {
			cst.setFixedCHARAtName("use_filter", use_filter);
		}
		if (procedure.contains(":mkd_id")) {
			cst.setFixedCHARAtName("mkd_id", mkd_id);
		}
		logger.debug("Execute command: " + procedure);
		cst.executeUpdate();
	}
	
	
	/**
	 * Выполнение SQL-запроса
	 * @return Результат типа <code>ResultSet</code>.
	 * @throws SQLException Выбрасывается при возникновении ошибки в SQL-запросе.
	 */
	public ResultSet executeSelect() throws SQLException {
		logger.trace("Invoke executeSelect()");
		if (cst != null) {
			cst.close();
			cst = null;
		}
		
		cst = (OraclePreparedStatement) connection.prepareStatement(select.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		cst.setQueryTimeout(60 * 60);
		if (select.contains(":pleskgesk")) {
			cst.setFixedCHARAtName("pleskgesk", pleskgesk);
		}
		if (select.contains(":pdb_lesk")) {
			cst.setFixedCHARAtName("pdb_lesk", pdb_lesk);
		}
		if (select.contains(":pdat")) {
			cst.setDateAtName("pdat", pdat);
		}
		if (select.contains(":use_filter")) {
			cst.setFixedCHARAtName("use_filter", use_filter);
		}
		if (select.contains(":mkd_id")) {
			cst.setFixedCHARAtName("mkd_id", mkd_id);
		}

		logger.debug("Original SQL: " + cst.getOriginalSql());
		return cst.executeQuery();
	}
	
	/**
	 * This method closed current connection.
	 * @throws SQLException Caused is during closing the connection there was a problem.
	 */
	public void close() throws SQLException {
		logger.trace("Invoke close()");
		cst.close();
		logger.trace("CallableStatement closed");
	}
}
