/**
 * 
 */
package ru.nlmk_it.db2file.exporters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;

/**
 * @author kosyh_ev
 *
 */
public class CSVExporter extends Exporter {
	
	private final Logger logger = Logger.getLogger(this.getClass());
	
	/**
	 * Символ конца строки в CSV-файле.
	 */
	private static final String endLine = "\n";
	
	/**
	 * Разделитель ячеек в CSV-файле.
	 */
	private static final String separator = "\t";
	
	/**
	 * Кодировка CSV-файла.
	 */
	private static final String encoding = "Cp1251";
	
	/**
	 * Формат даты в CSV-файле.
	 */
	private static final DateFormat format = new SimpleDateFormat("dd.MM.yyyy");
	
	/**
	 * Флаг, указывающий на необходимость добавления заголовков в файл.
	 */
	protected final boolean addFieldNames;
	
	/**
	 * Количество столбцов в создаваемом файле.
	 */
	private int numberOfColumns;
	
	/**
	 * Счётчик строк.
	 */
	private long counterOfRows;

	/**
	 * 
	 * @param outputFile
	 */
	protected CSVExporter(File exportDir,
		       String exportFilename,
		       String exportFileExtention,
		       boolean addFieldNames) {
		super(exportDir, exportFilename, exportFileExtention);
		this.addFieldNames = addFieldNames;
	}

	/**
	 * @see ru.nlmk_it.db2file.exporters.Exporter#export(java.sql.ResultSet)
	 */
	@Override
	public void export(ResultSet resultSet) throws IOException, SQLException {
		logger.trace("Invoke export()");
		
		boolean stillHaveKvitances = resultSet.first();
		do {
			Writer writer = null;
			try {
				File f = createNextExportFile(stillHaveKvitances ? resultSet.getString("BD_LESK") : "00");
				writer = openNewWriter(f, resultSet);
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
				writer.flush();
			    writer.close();
			}
		}
		while (stillHaveKvitances);
	}
	
	/**
	 * Создание потока записи, связанного с новым файлом.
	 * @param exportFile Файл, для которого нужно создать поток.
	 * @param resultSet результат SQL-запроса.
	 * @return объект-писатель.
	 * @throws IOException ошибки работы с файлом.
	 * @throws SQLException ошибки работы с БД.
	 */
	private Writer openNewWriter(File exportFile, ResultSet resultSet) throws IOException, SQLException {
		logger.trace("Invoke openNewWriter()");

		Writer writer = new BufferedWriter(
				new OutputStreamWriter(
						new FileOutputStream(exportFile), encoding));
		createFields(writer, resultSet);
		return writer;
	}
	
	/**
	 * Задание полей в CSV-файле.
	 * @param writer объект-писатель.
	 * @param resultSet результат SQL-запроса.
	 * @throws IOException ошибки работы с файлом.
	 * @throws SQLException ошибки работы с БД.
	 */
	private void createFields(Writer writer, ResultSet resultSet) throws IOException, SQLException {
		logger.trace("Invoke setFields()");
		ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
		numberOfColumns = resultSetMetaData.getColumnCount();
		counterOfRows = 0;
		
		if (addFieldNames) {
			for (int i = startColumnIndex; i < numberOfColumns; i++) {
				writer.append(resultSetMetaData.getColumnLabel(i + 1) + separator);
			}
			writer.append(endLine);
			counterOfRows++;
		}
	}
	
	
	/**
	 * Добавление строк из {@code ResultSet} в CSV-файл.
	 * @param writer объект-писатель.
	 * @param resultSet результат SQL-запроса.
	 * @return количество строк, добавленных в файл.
	 * @throws IOException ошибки работы с файлом.
	 * @throws SQLException ошибки работы с БД.
	 */
	private long putRows(Writer writer, ResultSet resultSet, boolean stillHaveKvitances) throws IOException, SQLException {
		logger.trace("Invoke putRows()");
		
		if (!stillHaveKvitances || resultSet.isAfterLast()) {
			return 0;
		}
		
		String prev_postal = resultSet.getString("postal");
		String prevaddressshort = resultSet.getString("addressshort");
		String prev_bd_lesk = resultSet.getString("bd_lesk");
		
		do {
			
			// Если код базы меняется, то нужно открыть новый файл.
			String bd_lesk = resultSet.getString("bd_lesk") == null ? "" : resultSet.getString("bd_lesk");
			String postal = resultSet.getString("postal") == null ? "" : resultSet.getString("postal");
			
			if (!bd_lesk.equalsIgnoreCase(prev_bd_lesk) || !postal.equalsIgnoreCase(prev_postal)) {
				if (counterOfRows > 0) {
					break;
				}
				else {
					prev_bd_lesk = bd_lesk;
					prev_postal = postal;
				}
			}
			
			
			// Если меняется улица, то в случае преодоления порогового значения открывается новый файл.
			String addressshort = resultSet.getString("addressshort") == null ? "" : resultSet.getString("addressshort");
			
			if (!addressshort.equalsIgnoreCase(prevaddressshort)) {
				if (counterOfRows >= SEPARATION_BORDER) {
					break;
				}
				else {
					prevaddressshort = addressshort;
				}
			}
			
			for (int i = startColumnIndex; i < numberOfColumns; i++) {
				// Первые два столбца (bd_lesk и addressshort) несут вспомогательное значение и в выгрузке не участвуют.
				Object obj = resultSet.getObject(i + 1);
				
				String cell = null;
				if (obj == null) {
					cell = separator;
				}
				else if (obj instanceof String) {
					String s = (String) obj;
					if (s.indexOf("\t") >= 0) {
						s = "\"" + s + "\""; // Спецсимволы обрамляем кавычками.
					}
					cell = s + separator;
				}
				else if (obj instanceof java.sql.Date) {
					cell = format.format(
									new java.util.Date(
											((java.sql.Date) obj).getTime())) + separator;
				}
				else if (obj instanceof BigDecimal) {
					cell = ((BigDecimal) obj).toPlainString() + separator;
				}
				else if (obj instanceof BigInteger) {
					cell = ((BigInteger) obj).toString() + separator;
				}
				else {
					assert false : "Unknown data type: " + obj.getClass().getName();
				}
				
				writer.append(cell);
			}
			writer.append(endLine);
			counterOfRows++;
		} while (resultSet.next());
		
		return counterOfRows;
	}
}
