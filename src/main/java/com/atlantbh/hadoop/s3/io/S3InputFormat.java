package com.atlantbh.hadoop.s3.io;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Abstract S3 File Input format class
 * 
 * @author samir
 * 
 * @param <K>
 *            Mapper key class
 * @param <V>
 *            Mapper value class
 */
public abstract class S3InputFormat<K, V> extends InputFormat<K, V> {

	Logger LOG = LoggerFactory.getLogger(S3InputFormat.class);

	public static String S3_BUCKET_NAME = "s3.bucket.name";
	/**
	 * Number of files to get from S3 in single request. Default value is 100
	 */
	public static String S3_KEY_PREFIX = "s3.key.prefix";
	public static String S3_MAX_KEYS = "s3.max.keys";

	public static String S3_NUM_OF_KEYS_PER_MAPPER = "s3.input.numOfKeys";

	S3BucketReader s3Reader;

	public S3InputFormat() throws IOException {
	}

	/**
	 * Returns list of {@link S3InputSplit}
	 * 
	 * @see org.apache.hadoop.mapreduce.InputFormat#getSplits(org.apache.hadoop.mapreduce.JobContext)
	 */
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();

		String bucketName = conf.get(S3_BUCKET_NAME);
		String keyPrefix = conf.get(S3_KEY_PREFIX);

		int maxKeys = conf.getInt(S3_MAX_KEYS, 1000);

		int numOfKeysPerMapper = conf.getInt(S3_NUM_OF_KEYS_PER_MAPPER, 1000);
		if (bucketName == null || "".equals(bucketName)) {
			String inputPath =  conf.get("mapred.input.dir");
			if (inputPath == null)
			try {
				setInputPath(conf, inputPath);
			} catch (URISyntaxException e) {
				throw new InvalidJobConfException("Bad mapred.input.dir property");			}
		}

		s3Reader = new S3BucketReader(bucketName, keyPrefix, null, maxKeys, new ConfigCredentials(conf));

		List<InputSplit> splits = new ArrayList<InputSplit>();

		S3ObjectSummary startKey = null;
		S3ObjectSummary endKey = null;

		int batchSize = 0;

		int numOfSplits = 0;
		int numOfCalls = 0;

		int maxKeyIDX = 0;
		int currentKeyIDX = 0;
		int nextKeyIDX = 0;

		ObjectListing listing = null;
		boolean isLastCall = true;

		// split all keys starting with "keyPrefix" into splits of
		// "numOfKeysPerMapper" keys
		do {
			// for first time we have to build request after that use
			// previous listing to get next batch
			if (listing == null) {
				listing = s3Reader.listObjects(bucketName, keyPrefix, maxKeys);
			} else {
				listing = s3Reader.listObjects(listing);
			}

			// Is this last call to WS (last batch of objects)
			isLastCall = !listing.isTruncated();

			// Size of the batch from last WS call
			batchSize = listing.getObjectSummaries().size();

			// Absolute index of last key from batch
			maxKeyIDX = numOfCalls * maxKeys + batchSize;

			// Absolute indexes of current and next keys
			currentKeyIDX = numOfSplits * numOfKeysPerMapper;
			// if there are no more keys to process, index of last key is
			// selected
			nextKeyIDX = (numOfSplits + 1) * numOfKeysPerMapper > maxKeyIDX && isLastCall ? maxKeyIDX : (numOfSplits + 1) * numOfKeysPerMapper;

			// create one input split for each key which is in current range
			while (nextKeyIDX <= maxKeyIDX) {

				startKey = endKey;
				endKey = listing.getObjectSummaries().get((nextKeyIDX - 1) % maxKeys);

				// Create new input split
				S3InputSplit split = new S3InputSplit();
				split.setBucketName(bucketName);
				split.setKeyPrefix(keyPrefix);
				split.setMarker(startKey != null ? startKey.getKey() : null);
				split.setLastKey(endKey.getKey());
				split.setSize(nextKeyIDX - currentKeyIDX);

				splits.add(split);
				numOfSplits++;

				// Stop when last key was processed
				if (nextKeyIDX == maxKeyIDX && isLastCall) {
					break;
				}

				// Set next key index
				currentKeyIDX = numOfSplits * numOfKeysPerMapper;
				nextKeyIDX = (numOfSplits + 1) * numOfKeysPerMapper > maxKeyIDX && isLastCall ? maxKeyIDX : (numOfSplits + 1) * numOfKeysPerMapper;
			}
			numOfCalls++;
		} while (!isLastCall);

		LOG.info("Number of input splits={}", splits.size());

		return splits;
	}
	
	public static void setInputPath(Configuration config, String path) throws URISyntaxException{
		URI uri = new URI(path);
		String bucket = uri.getHost();
		String keyPrefix = uri.getPath().substring(1);
		config.set(S3_BUCKET_NAME, bucket);
		config.set(S3_KEY_PREFIX, keyPrefix);
	}
	
	public static void setKeysPerMapper(Configuration config, int numKeys){
		config.setInt(S3_NUM_OF_KEYS_PER_MAPPER, numKeys);
	}
}
