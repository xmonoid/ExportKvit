/**
 * 
 */
package ru.nlmk_it.db2file.args;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

/**
 * The class-converter CLI value from {@link String} type to {@link FiletypeEnum} type.
 * 
 * @author kosyh_ev
 * @version 0.1
 * @see {@link IStringConverter}
 */
public final class FiletypeEnumConverter implements IStringConverter<FiletypeEnum> {

	@Override
	public FiletypeEnum convert(String value) {
		
		FiletypeEnum convertedValue = FiletypeEnum.fromString(value);
		
		if (convertedValue == null) {
			throw new ParameterException("Value " + value + " can't be converted to "
					+ FiletypeEnum.class.getName() + "."
							+ "Available values are: " + FiletypeEnum.valuesToString());
		}
		return convertedValue;
	}
}
