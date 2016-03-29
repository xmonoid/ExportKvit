/**
 * 
 */
package ru.nlmk_it.db2file;

import java.util.Arrays;

import org.apache.log4j.Logger;

import ru.nlmk_it.db2file.args.Arguments;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * The main class. Defines the program entry point. 
 * @author kosyh_ev
 * @version 0.1
 */
public final class ConsoleMain {

	private static final Logger logger = Logger.getLogger(ConsoleMain.class);
	
	/**
	 * @param args Arguments of the command line.
	 */
	public static void main(String[] args) {
		
		try {
			logger.info("Program started with arguments: " + Arrays.toString(args));
			Arguments arguments = new Arguments();
			
			// Check and setting CLI arguments.
			JCommander commander = new JCommander(arguments);
			try {
				commander.parse(args);
				
				// Print help and exit.
				if (arguments.isHelp()) {
					StringBuilder stringBuilder = new StringBuilder();
					commander.usage(stringBuilder);
					logger.info(stringBuilder);
					return;
				}
			}
			catch (ParameterException e) {
				logger.fatal(e);
				return;
			}
			
			// Create an exporter.
			DB2File exe = new DB2File(arguments);
			
			try {
				// Execute the export.
				exe.execute();
			}
			finally {
				// Close resources.
				exe.close();
			}
		}
		catch (Throwable t) {
			logger.fatal("Fatal error: ", t);
		}
	}

}
