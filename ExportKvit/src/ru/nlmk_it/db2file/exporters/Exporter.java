/**
 * 
 */
package ru.nlmk_it.db2file.exporters;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import ru.nlmk_it.db2file.args.Arguments;
import ru.nlmk_it.db2file.args.FiletypeEnum;

/**
 * @author kosyh_ev
 *
 */
public abstract class Exporter {
	
	private final Logger logger = Logger.getLogger(this.getClass());
	
	/**
	 * Каталог, в который сохраняются файлы.
	 */
	private final File exportDir;
	
	/**
	 * Имя экспортируемого файла по умолчанию.
	 */
	private final String exportFilename;
	
	/**
	 * Расширение экспортируемого файла.
	 */
	private final String exportFileExtention;
	
	/**
	 * Количество строк, после которого файл будет разделён.
	 */
	protected static final int SEPARATION_BORDER = 1000;
	
	/**
	 * Для разделения выгрузки нам нужны два служебных поля,
	 * которые в файлах выгрузки присутствовать не должны.
	 * Поэтому первые два столбца пропускаются.
	 */
	protected static final int startColumnIndex = 3;
	
	protected Exporter(File exportDir,
		       String exportFilename,
		       String exportFileExtention) {
		logger.trace("Create an object: " + this.toString());
		this.exportDir = exportDir;
		this.exportFilename = exportFilename;
		this.exportFileExtention = exportFileExtention;
	}
	
	
	/**
	 * The factory-method that creates the appropriate exporter.
	 * @param filetype
	 * @param exportFile
	 * @return
	 */
	public static Exporter getExporter(final Arguments arguments) {
		
		final File exportDir = arguments.getExportDir();
		final String exportFilename = arguments.getFilename();
		final FiletypeEnum filetype = arguments.getFiletype();
		
		if (filetype == FiletypeEnum.DBF) {
			return new DBFExporter(exportDir, exportFilename, filetype.toString().toLowerCase());
		}
		else if (filetype == FiletypeEnum.CSV) {
			return new CSVExporter(exportDir, exportFilename, filetype.toString().toLowerCase(), !arguments.isWithoutTitles());
		}
		else {
			assert false : filetype.toString().toLowerCase() + ": unknown filetype";
			return null;
		}
	}
	
	
	/**
	 * Создание нового файла для экспорта данных.
	 * @return объект созданного файла.
	 * @throws IOException
	 */
	protected File createNextExportFile(String bd_lesk) throws IOException {
		logger.trace("Invoke createExportFile()");
		
		File exportFile = new File(exportDir.getCanonicalPath() + File.separator + exportFilename + "_" + bd_lesk + "." + exportFileExtention);
		
		int i = 1;
		while (exportFile.exists()) {
			logger.debug("File " + exportFile.getName() +" is already exists.");
			exportFile = new File(exportDir.getCanonicalPath() + File.separator + exportFilename + "_" + bd_lesk + "_" + i++ + "." + exportFileExtention);
			logger.debug(" Create an other file: " + exportFile.getName());
		}
		
		logger.trace("createExportFile() returned" + exportFile.getCanonicalPath());
		return exportFile;
	}
	
	
	/**
	 * Export one result set.
	 * @param resultSet
	 * @throws IOException 
	 * @throws SQLException
	 */
	public abstract void export(ResultSet resultSet) throws IOException, SQLException;
}
