/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.sage;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.ParseException;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.StringParser;
import org.apache.flink.types.parser.StringValueParser;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sdk.clovis.ClovisAPI;


/**
 * {@link InputFormat} implementation that enables the access to the Mero Storage. 
 * It defines the way Mero Object will be read by multiple workers, i.e. 
 * how the input splits will be formed and how the records will be read from each
 * particular input split.
 * 
 * This implementation is working with CSV data format.
 * 
 * Each InputSplit comprises {@link ClovisInputFormat#buffersPerSplit} Mero data blocks.
 *
 * @param <T> the type of the elements this InputFormat produces
 */
public class ClovisInputFormat<T> extends RichInputFormat<T, ClovisInputSplit> {
	
	private static final Logger LOG = LoggerFactory.getLogger(ClovisInputFormat.class);
	
	private static final long serialVersionUID = 1L;
	
	private static final Class<?>[] EMPTY_TYPES = new Class<?>[0];
	
	private static final byte[] DEFAULT_LINE_DELIMITER = {'\n'};
	private static final byte[] DEFAULT_FIELD_DELIMITER = new byte[] {','};

	// The charset used to convert strings to bytes
	private String charsetName = "UTF-8";

	// Charset is not serializable
	private transient Charset charset;
	
	private static final byte BACKSLASH = 92;
	
	/**
	 * Maximum number of buffers in the read queue
	 */
	private static final int BUFFER_QUEUE_CAPACITY = 20;
	
	/**
	 * Maximum number of times the read task will be re-scheduled upon failure
	 */
	private static final int READ_RETRY_ATTEMPTS = 2;
	
	/**
	 * Periods the nextRecord method will wait to get the filled
	 * buffer from the queue between checking for the presence
	 * of failed ReadTasks
	 */
	private static final int BUFFER_WAIT_TIMEOUT_SEC = 5;
	
	private static final boolean[] EMPTY_INCLUDED = new boolean[0];
	
	private byte[] fieldDelim = DEFAULT_FIELD_DELIMITER;
	private byte[] recordDelim = DEFAULT_LINE_DELIMITER;
	
	private Class<?>[] fieldTypes = EMPTY_TYPES;
	
	protected boolean[] fieldIncluded = EMPTY_INCLUDED;
	
	private Path path;
	
	/**
	 * Number of buffers per one split
	 */
	private Integer buffersPerSplit;
	
	/**
	 * CSV parsing parameters
	 */
	private boolean lenient;
	private boolean quotedStringParsing = false;
	private byte quoteCharacter;
	private transient FieldParser<?>[] fieldParsers;
	protected transient Object[] parsedValues;
	private TupleSerializerBase<T> tupleSerializer;
	
	/**
	 * Blocking queue with buffers already filled
	 */
	private transient BlockingQueue<ClovisBuffer> fullBufferQueue;
	
	/**
	 * Holds clean buffers for reuse
	 */
	private transient LinkedList<ClovisBuffer> cleanBuffers;
	
	/**
	 * Asynchronous tasks executor
	 */
	private transient ClovisThreadPoolExecutor executor;
	
	/**
	 * The buffer this InputFormat currently reads from
	 */
	private transient ClovisBuffer currentBuffer;
	
	/**
	 * Holds the list of paths not assigned to asynchronous tasks yet
	 */
	private transient Iterator<Path> splitsIterator;
	
	/**
	 * Signalizes that all the splits were read
	 */
	private transient boolean end;
	
	/**
	 * The desired number of splits, as set by the configure() method.
	 */
	protected int numSplits = -1;



	/*********************************
	 * SAGE CLOVIS JAVA API PARAMETERS
	 *********************************/

	/**
	 * Id for an Object in Mero Storage
	 */
	private long meroObjectId;

	/**
	 * File path for the Object in Mero Storage
	 */
	private String meroFilePath;

	/**
	 * Size of the byte buffer specific to Object in Mero Storage
	 */
	private int meroBufferSize;

	/**
	 * Number of byte buffers holding the contents of the Object in Mero Storage
	 */
	private int meroChunkSize;

	/**
	 * Holds the offsets off the chunks of object to be read
	 */
	private transient Iterator<Integer> inputSplitIterator;


	
	public ClovisInputFormat() {}
	
	public ClovisInputFormat(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.path = filePath;
	}
	
	public ClovisInputFormat(Path filePath, int buffersPerSplit) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.path = filePath;
		this.buffersPerSplit = buffersPerSplit;
	}


	/**
	 * Constructor with parameters for SAGE Clovis API
	 */
	public ClovisInputFormat(long meroObjectId, String meroFilePath, int meroBufferSize, int meroChunkSize) {

		this.meroObjectId = meroObjectId;
		this.meroFilePath = meroFilePath;
		this.meroBufferSize = meroBufferSize;
		this.meroChunkSize = meroChunkSize;
	}

	public ClovisInputFormat(long meroObjectId, String meroFilePath, int meroBufferSize, int meroChunkSize, int buffersPerSplit) {

		this.meroObjectId = meroObjectId;
		this.meroFilePath = meroFilePath;
		this.meroBufferSize = meroBufferSize;
		this.meroChunkSize = meroChunkSize;
		this.buffersPerSplit = buffersPerSplit;
	}

	/**
	 * Precedence given to parameters provided in the configuration file
	 *
	 * Mero Object related properties should be assigned either using {@link #ClovisInputFormat(long, String, int, int)}
	 * or the TaskConfig
	 *
	 * {@link #buffersPerSplit} is to be set using {@link #ClovisInputFormat(long, String, int, int, int)}, or
	 * {@link #setBuffersPerSplit(Integer)}, or the TaskConfig
	 *
	 * @param parameters The configuration with all parameters (note: not the Flink config but the TaskConfig).
	 */
	@Override
	public void configure(Configuration parameters) {

		Long objectId = parameters.getLong(MERO_OBJECT_ID, 0L);
		if (objectId != 0L) {
			this.meroObjectId = objectId;
		}
		else if (this.meroObjectId == 0L) {
			throw new IllegalArgumentException("Mero object ID neither specified in input format, nor in configuration file");
		}

		String filePath = parameters.getString(MERO_FILE_PATH, null);
		if (filePath != null) {
			this.meroFilePath = filePath;
		}
		else if (meroFilePath == null) {
			throw new IllegalArgumentException("Mero object FilePath neither specified in input format, nor in configuration file");
		}

		int bufferSize = parameters.getInteger(MERO_BUFFER_SIZE, 0);
		if (bufferSize != 0) {
			this.meroBufferSize = bufferSize;
		}
		else if (meroBufferSize == 0) {
			throw new IllegalArgumentException("Mero object BufferSize neither specified in input format, nor in configuration file");
		}

		int chunkSize = parameters.getInteger(MERO_CHUNK_SIZE, 0);
		if (chunkSize != 0) {
			this.meroChunkSize = chunkSize;
		}
		else if (meroChunkSize == 0) {
			throw new IllegalArgumentException("Mero object ChunkSize neither specified in input format, nor in configuration file");
		}

		Integer buffPerSplit = parameters.getInteger(BUFFERS_PER_SPLIT_PARAMETER_KEY, -1);
		if (buffPerSplit > 0) {
			this.buffersPerSplit = buffPerSplit;
		} else if (buffersPerSplit == null) {
			throw new IllegalArgumentException("Number of buffers per split was not specified in input format, nor configuration.");
		}

		if (fieldTypes.length < 1) {
			throw new IllegalArgumentException("Field types are not configured");
		}
		
		/*//Read parameters from configuration
		String filePath = parameters.getString(FILE_PARAMETER_KEY, null);
		if (filePath != null) {
			try {
				this.path = new Path(filePath);
			}
			catch (RuntimeException rex) {
				throw new RuntimeException("Could not create a valid URI from the given file path name: " + rex.getMessage()); 
			}
		}
		else if (this.path == null) {
			//should be set through setter or configuration by now
			throw new IllegalArgumentException("File path was not specified in input format, nor configuration."); 
		}*/
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * TODO: Made changes to meet Clovis API specifications - InputSplit now contains an array of offset values rather than file paths.
	 * @param minNumSplits The minimum desired number of splits. If fewer are created, some parallel
	 *                     instances may remain idle.
	 * @return
	 * @throws IOException
	 */
	@Override
	public ClovisInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}
		
		// take the desired number of splits into account
		minNumSplits = Math.max(minNumSplits, this.numSplits);

		final List<ClovisInputSplit> inputSplits = new ArrayList<ClovisInputSplit>(minNumSplits);

		final int chunkSize = this.meroChunkSize;
		int numOfSplits = chunkSize/buffersPerSplit;
		if (chunkSize % buffersPerSplit > 0) {
			numOfSplits++;
		}

		if (numOfSplits < minNumSplits) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Impossible to produce" + minNumSplits + " splits");
			}
		}

		int splitNum = 0;
		int meroObjectOffset = 0;
		int meroSubObjectChunkSize = 4096;
		while (meroObjectOffset < this.meroBufferSize * this.meroChunkSize) {
			ArrayList<Integer> buffers = new ArrayList<>(buffersPerSplit);
			for (int i = 0; i < buffersPerSplit; i++) {
				buffers.add(meroObjectOffset);
				meroObjectOffset += meroSubObjectChunkSize;
			}

			ClovisInputSplit is = new ClovisInputSplit(splitNum++, buffers);
			inputSplits.add(is);

		}
		return inputSplits.toArray(new ClovisInputSplit[inputSplits.size()]);

		
		/*final Path path = this.path;
		final List<ClovisInputSplit> inputSplits = new ArrayList<ClovisInputSplit>(minNumSplits);

		// Get the list of files/buffers in this split
		List<FileStatus> files = new ArrayList<FileStatus>();

		final FileSystem fs = path.getFileSystem();
		final FileStatus pathFile = fs.getFileStatus(path);

		if (pathFile.isDir()) {
			addFilesInDir(path, files, true);
		} else {
			files.add(pathFile);
		}
		
		//Compute number of splits
		int bufferNum = files.size();
		int numOfSplits = bufferNum/buffersPerSplit;
		if (bufferNum % buffersPerSplit > 0) {
			numOfSplits++;
		}
		
		if (numOfSplits < minNumSplits) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Impossible to produce " + minNumSplits + " splits");
			}
		}
		
		//Create splits
		int splitNum = 0;
		Iterator<FileStatus> it = files.iterator();
		while (it.hasNext()) {
			ArrayList<Path> buffers = new ArrayList<Path>(buffersPerSplit);
			//Assure each split gets buffersPerSplit buffers, except the last one
			for (int i = 0; i < buffersPerSplit; i++) {
				if (it.hasNext()) {
					buffers.add(it.next().getPath());
				}
			}
			ClovisInputSplit is = new ClovisInputSplit(offset_buffers, splitNum++, buffers.toArray(new Path[buffers.size()]));
			inputSplits.add(is);
		}
		return inputSplits.toArray(new ClovisInputSplit[inputSplits.size()]);*/
	}
	
	/**
	 * Enumerate all files in the directory.
	 * @return the total length of accepted files.
	 */
	private long addFilesInDir(Path path, List<FileStatus> files, boolean logExcludedFiles)
			throws IOException {
		final FileSystem fs = path.getFileSystem();

		long length = 0;

		for(FileStatus file: fs.listStatus(path)) {
			if (!file.isDir()) {
				if(acceptFile(file)) {
					files.add(file);
					length += file.getLen();
				} else {
					if (logExcludedFiles && LOG.isDebugEnabled()) {
						LOG.debug("Directory "+file.getPath().toString()+" did not pass the file-filter and is excluded.");
					}
				}
			}
		}
		return length;
	}
	
	/**
	 * A simple hook to filter files and directories from the input.
	 * The method may be overridden. Hadoop's FileInputFormat has a similar mechanism and applies the
	 * same filters by default.
	 * 
	 * @param fileStatus The file status to check.
	 * @return true, if the given file or directory is accepted
	 */
	protected boolean acceptFile(FileStatus fileStatus) {
		final String name = fileStatus.getPath().getName();
		return !name.startsWith("_") && !name.startsWith(".");
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(ClovisInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(ClovisInputSplit split) throws IOException {
		
		//this.splitsIterator = Arrays.asList(split.getBuffers()).iterator();

		this.inputSplitIterator = split.getOffset_buffers().iterator();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Opening input split " + split.getSplitNumber());
		}
		
		// instantiate the csv field parsers - must be done in open as FileParser is not serializable
		FieldParser<?>[] parsers = new FieldParser<?>[fieldTypes.length];
		
		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] != null) {
				Class<? extends FieldParser<?>> parserType = FieldParser.getParserForType(fieldTypes[i]);
				if (parserType == null) {
					throw new RuntimeException("No parser available for type '" + fieldTypes[i].getName() + "'.");
				}

				FieldParser<?> p = InstantiationUtil.instantiate(parserType, FieldParser.class);

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
		
		// create the value holders
		this.parsedValues = new Object[fieldParsers.length];
		for (int i = 0; i < fieldParsers.length; i++) {
			this.parsedValues[i] = fieldParsers[i].createValue();
		}

		//instantiate the ThreadPoolExecutor
		
		if (executor == null || executor.isShutdown()) {
			this.executor = new ClovisThreadPoolExecutor(0, Integer.MAX_VALUE, 60L,
					TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
					READ_RETRY_ATTEMPTS);
		}
		
		//if fullBufferQueue was already created while reading previous InputSplit - just reuse it
		if (fullBufferQueue == null) {
			this.fullBufferQueue = new ArrayBlockingQueue<ClovisBuffer>(BUFFER_QUEUE_CAPACITY);
		}
		
		//reuse the buffers if they were already created while reading previous InputSplit
		if (this.cleanBuffers == null) {
			cleanBuffers = new LinkedList<ClovisBuffer>();
			for (int i = 0; i < BUFFER_QUEUE_CAPACITY; i++) {
				this.cleanBuffers.add(new ClovisBuffer());
			}
		}
		
		//Start BUFFER_QUEUE_CAPACITY read tasks to pre-read buffers
		for (int i = 0; i < BUFFER_QUEUE_CAPACITY; i++) {
			/*if (splitsIterator.hasNext()) {
				executor.execute(new ReadTask(cleanBuffers.pollLast(), fullBufferQueue, splitsIterator.next()));
			}*/
			if (inputSplitIterator.hasNext()) {
				executor.execute(new ReadTask(cleanBuffers.pollLast(), fullBufferQueue, inputSplitIterator.next()));
			}
		}
		
		this.end = false;
		
	}
	
	@Override
	public boolean reachedEnd() throws IOException {
		return end;
	}

	@Override
	public T nextRecord(T reuse) throws IOException {
		//if we finished reading records from current buffer
		if (currentBuffer == null || !currentBuffer.read(recordDelim)) {
			try {
				if (currentBuffer != null)  {
					//current buffer is now available for the next read task/ 
					//or if there are no more buffers to pre-read -- put it to clean buffers
					//to reuse when reading next InputSplit

					if (inputSplitIterator.hasNext()) {
						executor.execute(new ReadTask(currentBuffer, fullBufferQueue, inputSplitIterator.next()));

					/*if (splitsIterator.hasNext()) {
						executor.execute(new ReadTask(currentBuffer, fullBufferQueue, splitsIterator.next()));*/
					} else {
						cleanBuffers.add(currentBuffer);
						if (cleanBuffers.size() == BUFFER_QUEUE_CAPACITY) {
							currentBuffer = null;
							end = true;
							return null;
						}
					}
				}
				currentBuffer = null;
				while (currentBuffer == null) {
					
					//take the buffer from the blocking queue
					//if none available immediately - wait BUFFER_WAIT_TIMEOUT_SEC
					//in a loop while also checking the presence of execution errors
					currentBuffer = fullBufferQueue.poll(BUFFER_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS);
					if (executor.hasFailedTasks()) {
						//if there's a failed task - throw exception
						throw new IOException("Could not fill the buffer");
					}
				}
				return nextRecord(reuse);
			} catch (InterruptedException e) {
				throw new IOException("Could not fill the buffer");
			}
		}
		
		if (parseRecord(parsedValues, currentBuffer.array(), currentBuffer.getCurrentOffset(), currentBuffer.getCurrentLength())) {
			fillRecord(reuse, parsedValues);
		} else {
			return null;
		}
		return reuse;
	}
	
	/**
	 * Creates an object of type T from parsed field values
	 * @param reuse
	 * @param parsedValues - values of the fields in the tuple
	 * @return
	 */
	private T fillRecord(T reuse, Object[] parsedValues) {
		
		if (tupleSerializer == null)  {
			TypeInformation<T> typeInfo = TypeExtractor.getForObject(reuse);
			tupleSerializer = (TupleSerializerBase<T>) typeInfo.createSerializer(new ExecutionConfig());
		}
		
		return tupleSerializer.createOrReuseInstance(parsedValues, reuse);
	}
	

	@Override
	public void close() throws IOException {
		executor.shutdown();
		if (executor.hasFailedTasks()) {
			throw new IOException("Read was not successful");
		}
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
	
	public void setFields(int[] sourceFieldIndices, Class<?>[] fieldTypes) {
		Preconditions.checkNotNull(sourceFieldIndices);
		Preconditions.checkNotNull(fieldTypes);
		Preconditions.checkArgument(sourceFieldIndices.length == fieldTypes.length,
			"Number of field indices and field types must match.");

		for (int i : sourceFieldIndices) {
			if (i < 0) {
				throw new IllegalArgumentException("Field indices must not be smaller than zero.");
			}
		}

		int largestFieldIndex = max(sourceFieldIndices);
		this.fieldIncluded = new boolean[largestFieldIndex + 1];
		ArrayList<Class<?>> types = new ArrayList<Class<?>>();

		// check if we support parsers for these types
		for (int i = 0; i < fieldTypes.length; i++) {
			Class<?> type = fieldTypes[i];

			if (type != null) {
				if (FieldParser.getParserForType(type) == null) {
					throw new IllegalArgumentException("The type '" + type.getName()
						+ "' is not supported for the CSV input format.");
				}
				types.add(type);
				fieldIncluded[sourceFieldIndices[i]] = true;
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
	private boolean parseRecord(Object[] holders, byte[] bytes, int offset, int numBytes) throws ParseException {
		
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
								+ "Expect field types: "+fieldTypesToString() + " \n"
								+ "in file: " + path);
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
								+ "Expect field types: "+fieldTypesToString()+" \n"
								+ "in file: "+ path);
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

	public void setFilePath(Path filePath) {
		if (filePath == null) {
			throw new IllegalArgumentException("File path may not be null.");
		}
		
		this.path = filePath;
	}
	
	public void setFilePath(String filePath) {
		setFilePath(new Path(filePath));
	}
	
	public void setLenient(boolean lenient) {
		this.lenient = lenient;
	}
	
	public int getNumSplits() {
		return numSplits;
	}
	
	public void setNumSplits(int numSplits) {
		if (numSplits < -1 || numSplits == 0) {
			throw new IllegalArgumentException("The desired number of splits must be positive or -1 (= don't care).");
		}
		
		this.numSplits = numSplits;
	}
	
	public void enableQuotedStringParsing(char quoteCharacter) {
		quotedStringParsing = true;
		this.quoteCharacter = (byte)quoteCharacter;
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
	
	public void setBuffersPerSplit(Integer buffersPerSplit) {
		this.buffersPerSplit = buffersPerSplit;
	}
	
	public Integer getBuffersPerSplit() {
		return buffersPerSplit;
	}
	
	/**
	 * Asynchronous ReadTask
	 * Reads the data into the given ClovisBuffer and puts it into the blocking queue
	 * Task may be re-scheduled retryAttempt times if it fails, after that the read job 
	 * will be stopped.
	 *
	 * TODO: Need to add code to be able to read data from Mero. The 'buffer' should contain bytes retrived from Mero Storage
	 */
	public class ReadTask implements ClovisAsyncTask {
		
		private BlockingQueue<ClovisBuffer> queue;
		private ClovisBuffer buffer;
		private FSDataInputStream stream;
		private int retryAttempt;
		private ClovisAPI clovisApi = new ClovisAPI();



		int offset;
		
		public ReadTask(ClovisBuffer buffer, BlockingQueue<ClovisBuffer> queue, Path path) throws IOException {
			this.queue = queue;
			this.buffer = buffer;
			final FileSystem fs = FileSystem.get(path.toUri());
			this.stream = fs.open(path);
			this.retryAttempt = 0;
		}

		public ReadTask(ClovisBuffer clovisBuffer, BlockingQueue<ClovisBuffer> blockingQueue, int offset) {
			this.queue = blockingQueue;
			this.buffer = clovisBuffer;
			this.offset = offset;
			this.retryAttempt = 0;
		}

		@Override
		public void run() {
			try {
				//Clear buffer (reset the cursor)
				this.buffer.clear();

				int read = clovisApi.read(offset, this.buffer.getByteBuffer());
				//int read = this.stream.read(this.buffer.array());
				if (read == -1) {
					throw new RuntimeException("Buffer could not be filled");
				} else {
					//flip buffer (so that now it can be read from)
					buffer.flip();
					//set the limit to the amount of read bytes
					buffer.setLimit(read);
					//put into the blocking queue
					queue.put(buffer);
					//close the stream
					cleanup();
				}
			} catch (IOException | InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void cleanup() throws InterruptedException, IOException {
			final FSDataInputStream s = this.stream;
			if (s != null) {
				this.stream = null;
				s.close();
			}
		}

		@Override
		public int getRetryAttempt() {
			return retryAttempt;
		}

		@Override
		public void setRetryAttempt(int retryAttempt) {
			this.retryAttempt = retryAttempt;
		}
	}
	
	
	// ============================================================================================
	//  Parameterization via configuration
	// ============================================================================================
	
	// ------------------------------------- Config Keys ------------------------------------------
	
	/**
	 * The config parameter which defines the input file path.
	 */
	private static final String FILE_PARAMETER_KEY = "input.file.path";
	
	private static final String BUFFERS_PER_SPLIT_PARAMETER_KEY = "buffers.per.split";


	/**
	 * SAGE CLOVIS JAVA API CONFIG PARAMETERS
	 */

	private static final String MERO_OBJECT_ID = "mero.object.id";
	private static final String MERO_FILE_PATH = "mero.file.path";
	private static final String MERO_BUFFER_SIZE = "mero.buffer.size";
	private static final String MERO_CHUNK_SIZE = "mero.chunk.size";

	/*
	*  Implementation of max function for an array of int type values
	*/
	private static int max(int[] ints) {
		Preconditions.checkArgument(ints.length > 0);
		
		int max = ints[0];
		for (int i = 0; i < ints.length; i++) {
			max = Math.max(max, ints[i]);
		}
		return max;
	}

	public Charset getCharset() {
		if (this.charset == null) {
			this.charset = Charset.forName(charsetName);
		}
		return this.charset;
	}

}
