/**
 * 
 */
package ru.nlmk_it.db2file.database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

/**
 * В этом классе хранится сценарий выполнения, по результатам которого должа
 * сформироваться выгрузка данных.
 * 
 * @author kosyh_ev
 *
 */
public final class SQLScript {
	
	/**
	 * Логгер.
	 */
	private static final Logger logger = Logger.getLogger(SQLScript.class);
	
	private String procedure;
	
	private String select;
	
	private final String scriptFileName;
	
	/**
	 * Конструктор сценария.
	 * @param fileWithQueries - файл с SQL выражениями.
	 * @throws IOException выбрасывается при возникновении проблем чтения файла.
	 * @throws CustomSQLException выбрасывается при неверном выражении в сценарии.
	 */
	public SQLScript(File fileWithQueries) throws IOException, SQLException {
		logger.trace("Created an object: " + this.toString());
		assert fileWithQueries != null && fileWithQueries.isFile();
		
		this.scriptFileName = fileWithQueries.getName();
		StringBuilder fileData = new StringBuilder();
		
		// Чтение файла.
        BufferedReader reader = new BufferedReader(new FileReader(fileWithQueries));
        char[] buf = new char[1024];
        int numRead=0;
        while((numRead=reader.read(buf)) != -1){
            String readData = String.valueOf(buf, 0, numRead);
            fileData.append(readData);
        }
        reader.close();
        
        // Построение сценария.
		this.setScript(fileData);
	}
	
	
	/**
	 * Данный метод разбирает SQL сценарий на отдельные команды, создаёт соответствующие объекты,
	 * собирает их в список для последующего пошагового выполнения.
	 * @param script
	 * @throws SQLException
	 */
	private void setScript(StringBuilder script) throws SQLException {
		logger.trace("Invoke setScript()");
		assert script != null : "The file is empty";
		
		script = new StringBuilder(SQLUtils.replaceVarCharacter(script.toString(), '&', ':'));
		
		// Разделяем сценарий на отдельные выражения.
		String[] expressions = SQLUtils.allocateExpressions(script);
		
		for (String expression: expressions) {
			String first = expression.split("\\s", 2)[0];
			
			if (first.equalsIgnoreCase("select")) {
				if (select == null) {
					select = expression;
				}
				else {
					throw new SQLException("В сценарии присутствует больше одного select'а");
				}
			}
			else if (first.equalsIgnoreCase("call")) {
				if (procedure == null) {
					procedure = expression;
				}
				else {
					throw new SQLException("В сценарии присутствует больше одной процедуры"); 
				}
			}
		}
		
		if (select == null) {
			throw new SQLException("В сценарии отсутствует select");
		}
//		if (procedure == null) {
//			throw new SQLException("В сценарии отсутствует процедура");
//		}
	}
	
	public String getProcedure() throws SQLException {
		return procedure;
	}
	
	public String getSelect() {
		return select;
	}
	
	public String getScriptName() {
		return scriptFileName;
	}
}
