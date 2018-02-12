package org.apache.flink.api.sage;

import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.apache.flink.types.parser.StringValueParser;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * Created by Clemens Lutz on 2/12/18.
 */
public class Deserializer<T> implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(Deserializer.class);

	private String charsetName = "UTF-8";

	private transient Charset charset;

	private static final byte BACKSLASH = 92;

	private byte[] fieldDelim;

	private byte[] recordDelim;

	private Class<?>[] fieldTypes;

	protected boolean[] fieldIncluded;

	/**
	 * CSV parsing parameters
	 */
	private boolean lenient;
	private boolean quotedStringParsing = false;
	private byte quoteCharacter;
	private transient FieldParser<?>[] fieldParsers;
	private int invalidLineCount = 0;

	Deserializer(byte[] fieldDelim, byte[] recordDelim, Class<?>[] fieldTypes, boolean[] fieldIncluded) {

		this.fieldDelim = fieldDelim;
		this.recordDelim = recordDelim;
		this.fieldTypes = fieldTypes;
		this.fieldIncluded = fieldIncluded;

	}

	public void open() throws IOException {

		if (fieldTypes.length < 1) {
			throw new IllegalArgumentException("Field types are not configured");
		}

		// instantiate the parsers
		FieldParser<?>[] parsers = new FieldParser<?>[fieldTypes.length];

		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] != null) {
				Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
				}

				FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);

				p.setCharset(getCharset());
				if (this.quotedStringParsing) {
					if (p instanceof StringParser) {
						((StringParser)p).enableQuotedStringParsing(this.quoteCharacter);
					} else if (p instanceof StringValueParser) {
						((StringValueParser)p).enableQuotedStringParsing(this.quoteCharacter);
					}
				}

				parsers[i] = p;
			}
		}
		this.fieldParsers = parsers;
	}

	public void close() throws IOException {
//		if (this.invalidLineCount > 0) {
//			if (LOG.isWarnEnabled()) {
//				LOG.warn("In file \""+ this.filePath + "\" (split start: " + this.splitStart + ") " + this.invalidLineCount +" invalid line(s) were skipped.");
//			}
//		}
//
//		if (this.commentCount > 0) {
//			if (LOG.isInfoEnabled()) {
//				LOG.info("In file \""+ this.filePath + "\" (split start: " + this.splitStart + ") " + this.commentCount +" comment line(s) were skipped.");
//			}
//		}
//		super.close();
	}

	/**
	 * Set types of the fields in Tuple
	 * @param includedMask - bitmap of the fields that should be included
	 * @param fieldTypes
	 */
	public void setFields(boolean[] includedMask, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(includedMask);
		Preconditions.checkNotNull(fieldTypes);

		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if types are valid for included fields
		int typeIndex = 0;
		for (int i = 0; i < includedMask.length; i++) {

			if (includedMask[i]) {
				if (typeIndex > fieldTypes.length - 1) {
					throw new IllegalArgumentException("Missing type for included field " + i + ".");
				}
				Class<?> type = fieldTypes[typeIndex++];

				if (type == null) {
					throw new IllegalArgumentException("Type for included field " + i + " should not be null.");
				} else {
					// check if we support parsers for this type
					if (FieldParser.getParserForType(type) == null) {
						throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
					}
					types.add(type);
				}
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
		this.fieldIncluded = includedMask;
	}

	public void setFields(Class<?> ... fieldTypes) {
		if (fieldTypes == null) {
			throw new IllegalArgumentException("Field types must not be null.");
		}

		this.fieldIncluded = new boolean[fieldTypes.length];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];

			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName() + "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[i] = true;
			}
		}

		this.fieldTypes = types.toArray(new Class<?>[types.size()]);
	}

	/**
	 * Parse the byte array into fields of given types
	 * @param holders
	 * @param bytes
	 * @param offset
	 * @param numBytes
	 * @return
	 * @throws ParseException
	 */
	public boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {

		boolean[] fieldIncluded = this.fieldIncluded;

		int startPos = offset;
		final int limit = offset + numBytes;

		for (int field = 0, output = 0; field < fieldIncluded.length; field++) {

			// check valid start position
			if (startPos >= limit) {
				if (lenient) {
					return false;
				} else {
					throw new ParseException("Row too short: " + new String(bytes, offset, numBytes));
				}
			}

			if (fieldIncluded[field]) {
				// parse field
				@SuppressWarnings("unchecked")
				FieldParser<Object> parser = (FieldParser<Object>) this.fieldParsers[output];
				Object reuse = holders[output];
				startPos = parser.resetErrorStateAndParse(bytes, startPos, limit, this.fieldDelim, reuse);
				holders[output] = parser.getLastResult();

				// check parse result
				if (startPos < 0) {
					// no good
					if (lenient) {
						return false;
					} else {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: '" + lineAsString + "'\n"
							+ "ParserError " + parser.getErrorState() + " \n"
							+ "Expect field types: "+fieldTypesToString() + " \n");
					}
				}
				output++;
			}
			else {
				// skip field
				startPos = skipFields(bytes, startPos, limit, this.fieldDelim);
				if (startPos < 0) {
					if (!lenient) {
						String lineAsString = new String(bytes, offset, numBytes);
						throw new ParseException("Line could not be parsed: '" + lineAsString+"'\n"
							+ "Expect field types: "+fieldTypesToString()+" \n");
					}
				}
			}
		}
		return true;
	}

	protected int skipFields(byte[] bytes, int startPos, int limit, byte[] delim) {

		int i = startPos;

		final int delimLimit = limit - delim.length + 1;

		if (quotedStringParsing && bytes[i] == quoteCharacter) {

			// quoted string parsing enabled and field is quoted
			// search for ending quote character, continue when it is escaped
			i++;

			while (i < limit && (bytes[i] != quoteCharacter || bytes[i-1] == BACKSLASH)){
				i++;
			}
			i++;

			if (i == limit) {
				// we are at the end of the record
				return limit;
			} else if ( i < delimLimit && FieldParser.delimiterNext(bytes, i, delim)) {
				// we are not at the end, check if delimiter comes next
				return i + delim.length;
			} else {
				// delimiter did not follow end quote. Error...
				return -1;
			}
		} else {
			// field is not quoted
			while(i < delimLimit && !FieldParser.delimiterNext(bytes, i, delim)) {
				i++;
			}

			if (i >= delimLimit) {
				// no delimiter found. We are at the end of the record
				return limit;
			} else {
				// delimiter found.
				return i + delim.length;
			}
		}

	}

	private String fieldTypesToString() {
		StringBuilder string = new StringBuilder();
		string.append(this.fieldTypes[0].toString());

		for (int i = 1; i < this.fieldTypes.length; i++) {
			string.append(", ").append(this.fieldTypes[i]);
		}

		return string.toString();
	}

	public void setLenient(boolean lenient) {
		this.lenient = lenient;
	}

	public void setFieldDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}

		this.fieldDelim = delimiter;
	}

	public void setFieldDelimiter(char delimiter) {
		setFieldDelimiter(String.valueOf(delimiter));
	}

	public void setFieldDelimiter(String delimiter) {
		this.fieldDelim = delimiter.getBytes(getCharset());
	}

	public void setRecordDelimiter(byte[] delimiter) {
		if (delimiter == null) {
			throw new IllegalArgumentException("Delimiter must not be null");
		}

		this.recordDelim = delimiter;
	}

	public void setRecordDelimiter(char delimiter) {
		setRecordDelimiter(String.valueOf(delimiter));
	}

	public void setRecordDelimiter(String delimiter) {
		this.recordDelim = delimiter.getBytes(getCharset());
	}

	public Charset getCharset() {
		if (this.charset == null) {
			this.charset = Charset.forName(charsetName);
		}
		return this.charset;
	}

	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = (byte)quoteCharacter;
	}

	public void setFieldTypes(Class<?>[] fieldTypes) {
		this.fieldTypes = fieldTypes;
	}

}
