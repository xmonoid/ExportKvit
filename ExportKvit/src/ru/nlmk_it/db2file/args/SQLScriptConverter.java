/**
 * 
 */
package ru.nlmk_it.db2file.args;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import ru.nlmk_it.db2file.database.SQLScript;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

/**
 * This is class-converter of CLI value from {@link String} type to {@link SQLScript} type.
 * @author kosyh_ev
 * @version 0.1
 * @see {@link IStringConverter}
 */
public final class SQLScriptConverter implements IStringConverter<SQLScript> {
	
	private final Logger logger = Logger.getLogger(this.getClass());

	@Override
	public SQLScript convert(String value) {
		logger.trace("Invoke convert(" + value + ")");
		
		try {
			File f = new File(value);
			
			if (!f.isFile()) {
				throw new ParameterException(value + " - file not found");
			}
			
			if (!f.canRead()) {
				throw new ParameterException(value + " - unable to read file");
			}
			
			SQLScript result;
			
			try {
				result = new SQLScript(f);
			}
			catch (SQLException e) {
				logger.error(e);
				result = null;
			}
			return result;
		}
		catch (IOException e) {
			throw new ParameterException(e);
		}
	}
}
