/**
 * 
 */
package ru.nlmk_it.db2file.args;

import org.apache.log4j.Logger;

/**
 * This enumeration defines available types of export file.
 * 
 * @version 0.1
 * @author kosyh_ev
 *
 */
public enum FiletypeEnum {
	
	XLSX,
	
	DBF,
	
	CSV;
	
	private static final Logger logger = Logger.getLogger(FiletypeEnum.class);
	
	/**
	 * 
	 * @param code The desired type in the {@link String} format.
	 * @return The desired type in the {@link FiletypeEnum} format or {@code null} if specified type does not supported.
	 */
	public static FiletypeEnum fromString(String code) {
		logger.trace("Invoke fromString("+ code +")");
		for (FiletypeEnum type: FiletypeEnum.values()) {
			if (type.toString().equalsIgnoreCase(code)) {
				return type;
			}
		}
		return null;
	}
	
	/**
	 * Supported types.
	 * @return List of supported types in the form of a comma-separated {@link String}.
	 */
	public static String valuesToString() {
		logger.trace("Invoke valuesToString()");
		String list = new String();
		for (FiletypeEnum type: FiletypeEnum.values()) {
			list += ", " + type.toString().toLowerCase();
		}
		return list.substring(2);
	}
}
