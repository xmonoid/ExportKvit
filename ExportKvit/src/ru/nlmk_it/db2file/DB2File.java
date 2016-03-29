/**
 * 
 */
package ru.nlmk_it.db2file;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import ru.nlmk_it.db2file.args.Arguments;
import ru.nlmk_it.db2file.database.SQLExecutor;
import ru.nlmk_it.db2file.database.SQLScript;
import ru.nlmk_it.db2file.exporters.Exporter;

/**
 * Данный класс является ключевым при выгрузке данных. Именно он инициирует этот процесс.
 * При создании класса его конструктору передаётся объект типа {@link Arguments}, в котором
 * указываются все необходимые параметры выгрузки.
 * 
 * @author kosyh_ev
 *
 */
public final class DB2File {

	/**
	 * Логгер.
	 */
	private static final Logger logger = Logger.getLogger(DB2File.class);

	/**
	 * Здесь хранится подключение к БД.
	 */
	private Connection connection;

	/**
	 * Здесь хранятся параметры запуска приложения.
	 */
	private Arguments arguments;

	{
		Properties properties = new Properties();
		InputStream in = null;
		try {
			in = new FileInputStream("./etc/parameters.property");
			properties.load(in);
			
			String url = properties.getProperty("url");
			String login = properties.getProperty("login");
			String password = properties.getProperty("password");
			connection = DriverManager.getConnection(url, login, password);
			logger.info("Database connection created.");
			logger.debug("\turl = " + url + "\n\tlogin = " + login);
			in.close();
		}
		catch (IOException e) {
			logger.fatal(e);
			throw new RuntimeException(e);
		}
		catch (SQLException e) {
			logger.fatal(e);
			throw new RuntimeException(e);
		}
		finally {
			try {
				if (in != null) {
					in.close();
				}
			}
			catch (IOException e) {
				logger.error(e);
			}
		}
	}
	
	/**
	 * Конструктор создаёт подключение к БД, а также создаёт файл с будущей выргузкой.
	 * 
	 * @param arguments - парамтеры выгрузки.
	 * @throws SQLException ошибки работы с БД.
	 */
	public DB2File(Arguments arguments) throws SQLException {
		logger.trace("Created an object: " + this.toString());
		arguments.validate();
		this.arguments = arguments;
	}


	/**
	 * Метод запускает выгрузку.
	 * 
	 * @throws IOException ошибки работы с файлом.
	 * @throws SQLException ошибки работы с БД.
	 */
	public void execute() throws SQLException, IOException {
		logger.trace("Invoke execute()");

		SQLScript script = arguments.getQueries();
		
		String procedure = script.getProcedure();
		String select = script.getSelect();
		
		SQLExecutor executor = new SQLExecutor(connection, procedure, select);
		
		executor.setPleskgesk(arguments.getPleskgesk());
		executor.setDbLesk(arguments.getPdb_lesk());
		executor.setPdat(arguments.getPdat());
		executor.setPnotEmpty(arguments.getPnot_empty());
		executor.setUseFilter(arguments.getUseFilter());
		executor.setMkdId(arguments.getMkdId());
		
		if (procedure != null && !arguments.isNoProcedure()) {
			executor.executeProcedure();
		}
		ResultSet resultSet = executor.executeSelect();
		
		Exporter exporter = Exporter.getExporter(arguments);
		
		exporter.export(resultSet);
		
		executor.close();
	}


	/**
	 * Close connection to database.
	 * @throws SQLException Cause if a database access error occurs.
	 */
	public void close() throws SQLException {
		logger.trace("Invoke close()");
		connection.close();
		logger.info("Connection closed");
	}
}
