package com.atlantbh.hadoop.s3.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3.S3Exception;

import com.amazonaws.auth.AWSCredentials;

public class ConfigCredentials implements AWSCredentials {
	public static final String SECRET_KEY="fs.s3n.awsSecretAccessKey";
	public static final String ACCESS_KEY="fs.s3n.awsAccessKeyId";
	
	private String accessKey;
	private String secretKey;
	
	public ConfigCredentials(Configuration config) throws S3Exception{
		accessKey = config.get(ACCESS_KEY);
		secretKey = config.get(SECRET_KEY);
		if (accessKey == null || secretKey == null){
			throw new S3Exception(new IllegalArgumentException("Access Key ID and Secret Access Key must be specified"));
		}
	}

	@Override
	public String getAWSAccessKeyId() {
		return accessKey;
	}

	@Override
	public String getAWSSecretKey() {
		return secretKey;
	}

}
