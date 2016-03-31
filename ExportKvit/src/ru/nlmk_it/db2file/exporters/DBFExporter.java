/**
 * 
 */
package ru.nlmk_it.db2file.exporters;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;

/**
 * @author kosyh_ev
 *
 */
public final class DBFExporter extends Exporter {

	private final Logger logger = Logger.getLogger(this.getClass());
	
	/**
	 * Количество столбцов в создаваемом файле.
	 */
	private int numberOfColumns;
	
	/**
	 * Счётчик строк.
	 */
	private long counterOfRows;
	
	/**
	 * Кодировка DBF файла. 
	 */
	private static final String CHARSET_ENCODING = "Cp866";
	
	
	/**
	 * 
	 * @param outputFile
	 */
	protected DBFExporter(File exportDir,
		       String exportFilename,
		       String exportFileExtention) {
		super(exportDir, exportFilename, exportFileExtention);
	}

	/**
	 * @see ru.nlmk_it.db2file.exporters.Exporter#export(java.sql.ResultSet)
	 */
	@Override
	public void export(ResultSet resultSet) throws IOException, SQLException {
		logger.trace("Invoke export()");
		
		boolean stillHaveKvitances = resultSet.first();
		do {
		
			DBFWriter writer = null;
			OutputStream out = null;
			try {
				File f = createNextExportFile(stillHaveKvitances ? resultSet.getString("BD_LESK") : "00");
				writer = new DBFWriter(f);
				writer.setCharactersetName(CHARSET_ENCODING);
				writer.setFields(createFields(resultSet));
				out = new FileOutputStream(f);
				
				long count = putRows(writer, resultSet, stillHaveKvitances);
				// Exit loop if no more lines in ResultSet.
				if (stillHaveKvitances) {
					stillHaveKvitances = !resultSet.isAfterLast();
				}
				logger.info("It was added " + count + " rows into the file " + f.getName());
			}
			catch (Exception e) {
				logger.fatal(e.getMessage(), e);
			}
			finally {
				
				writer.write(out);
				out.close();
			}
		}
		while (stillHaveKvitances);
	}
	
	/**
	 * Задание полей в DBF-файле.
	 * @param resultSet результат SQL-запроса.
	 * @return массив полей.
	 * @throws DBFException ошибки работы с файлом.
	 * @throws SQLException ошибки работы с БД.
	 */
	private DBFField[] createFields(ResultSet resultSet) throws SQLException, DBFException {
		logger.trace("Invoke createFields()");
		List<DBFField> fields = new ArrayList<DBFField>();
		
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		numberOfColumns = resultSetMetaData.getColumnCount();
		counterOfRows = 0;
		logger.debug("numberOfColumns = " + numberOfColumns);
		for (int i = startColumnIndex; i < numberOfColumns; i++) {
			DBFField field = new DBFField();
			String name = resultSetMetaData.getColumnLabel(i + 1).length() < 10 ? resultSetMetaData.getColumnLabel(i + 1) : resultSetMetaData.getColumnLabel(i + 1).substring(0, 10);
			field.setName(name);
			logger.debug("Field name = " + name);
			int length = resultSetMetaData.getColumnDisplaySize(i + 1);
			if (length > 244) {
				length = 244;
			}
			logger.debug("Field name length = " + length);
			
			byte fieldType = getDBFType(resultSetMetaData.getColumnType(i + 1));
			field.setDataType(fieldType);
			
			if (fieldType == DBFField.FIELD_TYPE_C ||
				fieldType == DBFField.FIELD_TYPE_N) {
				field.setFieldLength(length < 1 ? 1 : length); // : To think about this.
			}
			
			fields.add(field);
		}
		
		return fields.toArray(new DBFField[fields.size()]);
	}

	
	/**
	 * Установка соответствия между типами Oracle и dBase.
	 * @param sqlType Тип Oracle.
	 * @return Тип dBase.
	 * @throws DBFException Соответствие установить не удалось.
	 */
	private final byte getDBFType(int sqlType) throws DBFException {
		logger.trace("Invoke getDBFType(" + sqlType + ")");
		byte result;
		
		switch (sqlType) {
		case Types.CHAR:
		case Types.VARCHAR:
			result = DBFField.FIELD_TYPE_C;
			break;
		case Types.INTEGER:
		case Types.DECIMAL:
		case Types.FLOAT:
		case Types.NUMERIC:
			result = DBFField.FIELD_TYPE_N;
			break;
		case Types.DATE:
		case Types.TIMESTAMP:
			result = DBFField.FIELD_TYPE_D;
			break;
		case Types.BOOLEAN:
			result = DBFField.FIELD_TYPE_L;
			break;
		default:
			throw new DBFException("Unknown SQL type");
		}
		
		return result;
	}
	
	/**
	 * Добавление строк из {@code ResultSet} в DBF-файл.
	 * @param writer объект-писатель.
	 * @param resultSet результат SQL-запроса.
	 * @return количество строк, добавленных в файл.
	 * @throws DBFException ошибки работы с файлом.
	 * @throws SQLException ошибки работы с БД.
	 */
	private long putRows(DBFWriter writer, ResultSet resultSet, boolean stillHaveKvitances) throws DBFException, SQLException {
		logger.trace("Invoke putRows()");
		
		if (!stillHaveKvitances || resultSet.isAfterLast()) {
			return 0;
		}
		
		String prev_postal = resultSet.getString("postal");
		String prev_bd_lesk = resultSet.getString("bd_lesk");
		
		do {
			
			// Если код базы меняется, то нужно открыть новый файл.
			String bd_lesk = resultSet.getString("bd_lesk") == null ? "" : resultSet.getString("bd_lesk");
			
			if (!bd_lesk.equalsIgnoreCase(prev_bd_lesk)) {
				if (counterOfRows > 0) {
					break;
				}
				else {
					prev_bd_lesk = bd_lesk;
				}
			}
			
			// Если меняется почтовый индекс, то в случае преодоления порогового значения открывается новый файл.
			String postal = resultSet.getString("postal") == null ? "" : resultSet.getString("postal");
			
			if (!postal.equalsIgnoreCase(prev_postal)) {
				if (counterOfRows >= SEPARATION_BORDER) {
					break;
				}
				else {
					prev_postal = postal;
				}
			}
			
			Object[] rowData = new Object[numberOfColumns];
			for (int i = startColumnIndex; i < numberOfColumns; i++) {
				// Первые два столбца (bd_lesk и addressshort) несут вспомогательное значение и в выгрузке не участвуют.
				Object object= resultSet.getObject(i + 1);
				
				if (object instanceof String) {
					rowData[i] = ((String) object);
				}
				else if (object instanceof BigDecimal) {
					double value = ((BigDecimal) object).doubleValue();
					rowData[i] = resultSet.wasNull() ? null : new Double(value);
				}
				else if (object instanceof BigInteger) {
					int value = ((BigInteger) object).intValue();
					rowData[i] = resultSet.wasNull() ? null : new Integer(value);
				}
				else {
					rowData[i] = object;
				}
			}
			writer.addRecord(rowData);
			counterOfRows++;
		} while (resultSet.next());
		return counterOfRows;
	}
}
