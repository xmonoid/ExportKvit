/**
 * 
 */
package ru.nlmk_it.db2file.database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * Класс содержит типовую функциональность, используемую для
 * обработки SQL выражений.
 * 
 * @author kosyh_ev
 *
 */
public final class SQLUtils {
	
	private static final Logger logger = Logger.getLogger(SQLUtils.class);
	
	private static char variableMarker = ':';
	
	public static Set<String> getVariables(String expression) throws SQLException {
		logger.trace("Invoke getVariables()");
		assert expression != null;
		
		StringBuilder copyExpression = new StringBuilder(expression);
		
		copyExpression.delete(0, 2);
//		{
//			int oneLineCommentStart = copyExpression.indexOf("--");
//			int twoLineCommentStart = copyExpression.indexOf("/*");
//			int stringStart = copyExpression.indexOf("'");
//			int aliasStart = copyExpression.indexOf("\"");
//		}
//		
		
		
		Set<String> result = new HashSet<String>();
		String[] words = copyExpression.toString().split("[,\\s\\(\\)=\\+;]");
		
		for (String word: words) {
			if (word != null && !word.isEmpty() && word.charAt(0) == variableMarker) {
				result.add(word.substring(1));
			}
		}
		
		return result;
	}
	
	
	/**
	 * This method replace symbol '&' of variable in Oracle SQL to standard symbol ':' of SQL variable.
	 */
	public static String replaceVarCharacter(String expression, char oldVarChar, char newVarChar) {
		logger.trace("Invoke replaceVarCharacter()");
		char[] symbol = expression.toCharArray();
		boolean isOneLineComment = false;
		boolean isMultLineComment = false;
		boolean isString = false;
		boolean isAlias = false;
		for (int i = 1; i < symbol.length; i++) {
			/* Нужно заменить все вхождения символа oldVarChar на newVarChar.
			 * Исключения:
			 * 1. Однострочные комментарии -- 
			 * 2. Многострочные комментарии /*
			 * 3. Строки '...'
			 * 4. Алиасы "..."
			 */
			
			if (!isOneLineComment) {
				if (!isMultLineComment) {
					if (!isString) {
						if (!isAlias) {
							
							if (symbol[i] == '-' && symbol[i-1] == '-') {
								isOneLineComment = true;
							}
							else if (symbol[i-1] == '/' && symbol[i] == '*') {
								isMultLineComment = true;
							}
							else if (symbol[i] == '\'') {
								isString = true;
							}
							else if (symbol[i] == '"') {
								isAlias = true;
							}
							// It's for which all was started.
							else if (symbol[i] == oldVarChar) {
								symbol[i] = newVarChar;
							}
						}
						else {
							// End of alias.
							if (symbol[i] == '"') {
								isAlias = false;
							}
						}
					}
					else {
						// End of string.
						if (symbol[i] == '\'') {
							isString = false;
						}
					}
				}
				else {
					// End of multiply line comment.
					if (symbol[i-1] == '*' && symbol[i] == '/') {
						isMultLineComment = false;
					}
				}
			}
			else {
				// End of one line comment.
				if (symbol[i] == '\n') {
					isOneLineComment = false;
				}
			}
		}
		
		
		return new String(symbol);
	}
	
	
	/**
	 * Allocate SQL script to separate SQL expressions. This method does not check
	 * the correct of expressions.
	 * @param script SQL script.
	 * @return separate SQL expressions.
	 */
	public static String[] allocateExpressions(StringBuilder script) {
		logger.trace("Invoke allocateExpressions()");
		assert script != null && script.length() != 0 : "The script is empty";
		
		List<String> result = new ArrayList<String>();
		String sScript = script.toString();
		char[] symbol = sScript.toCharArray();
//		boolean isOneLineComment = false;
//		boolean isMultLineComment = false;
//		boolean isString = false;
//		boolean isAlias = false;
		int j = 0;
		for (int i = 1; i < symbol.length; i++) {
			/* Нужно разделить сценарий по содержащимся символам ';'.
			 * Исключения:
			 * 1. Однострочные комментарии -- 
			 * 2. Многострочные комментарии /*
			 * 3. Строки '...'
			 * 4. Алиасы "..."
			 */
//			if (!isOneLineComment) {
//				if (!isMultLineComment) {
//					if (!isString) {
//						if (!isAlias) {
							/*if (symbol[i] == '-' && symbol[i-1] == '-') {
								isOneLineComment = true;
							}
							else if (symbol[i-1] == '/' && symbol[i] == '*') {
								isMultLineComment = true;
							}
							else if (symbol[i] == '\'') {
								isString = true;
							}
							else if (symbol[i] == '"') {
								isAlias = true;
							}
							else */if (symbol[i] == ';') {
								result.add(sScript.substring(j, i).trim());
								j = i + 1;
							}
						}
//						else {
//							// End of alias.
//							if (symbol[i] == '"') {
//								isAlias = false;
//							}
//						}
//					}
//					else {
//						// End of string.
//						if (symbol[i] == '\'') {
//							isString = false;
//						}
//					}
//				}
//				else {
//					// End of multiply line comment.
//					if (symbol[i-1] == '*' && symbol[i] == '/') {
//						isMultLineComment = false;
//					}
//				}
//			}
//			else {
//				// End of one line comment.
//				if (symbol[i] == '\n') {
//					isOneLineComment = false;
//					j = i + 1;
//				}
//			}
//		}
		
		return result.toArray(new String[result.size()]);
	}
}
