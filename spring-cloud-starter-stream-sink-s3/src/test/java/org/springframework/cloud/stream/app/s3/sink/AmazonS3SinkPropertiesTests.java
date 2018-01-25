/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.stream.app.s3.sink;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.Test;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cloud.stream.config.SpelExpressionConverterConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Import;

import com.amazonaws.services.s3.model.CannedAccessControlList;

/**
 * @author Artem Bilan
 */
public class AmazonS3SinkPropertiesTests {

	@Test
	public void s3BucketCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("s3.bucket:foo")
				.applyTo(context);
		context.register(Conf.class);
		context.refresh();
		AmazonS3SinkProperties properties = context.getBean(AmazonS3SinkProperties.class);
		assertThat(properties.getBucket(), equalTo("foo"));
		context.close();
	}

	@Test
	public void s3BucketExpressionCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("s3.bucket-expression:headers.bucket")
				.applyTo(context);
		context.register(Conf.class);
		context.refresh();
		AmazonS3SinkProperties properties = context.getBean(AmazonS3SinkProperties.class);
		assertThat(properties.getBucketExpression().getExpressionString(), equalTo("headers.bucket"));
		context.close();
	}

	@Test
	public void s3BucketAndBucketExpressionAreMutuallyExclusive() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("s3.bucket:foo", "s3.bucketExpression:headers.bucket")
				.applyTo(context);
		context.register(Conf.class);
		try {
			context.refresh();
			fail("BeanCreationException expected");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(BeanCreationException.class));
			assertThat(e.getCause(), instanceOf(BindException.class));
			assertThat(e.getCause().getCause(), instanceOf(BindValidationException.class));
		}
		context.close();
	}

	@Test
	public void s3KeyCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("s3.bucket:foo", "s3.keyExpression:'foo'")
				.applyTo(context);
		context.register(Conf.class);
		context.refresh();
		AmazonS3SinkProperties properties = context.getBean(AmazonS3SinkProperties.class);
		assertThat(properties.getKeyExpression().getExpressionString(), equalTo("'foo'"));
		context.close();
	}

	@Test
	public void aclCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("s3.bucket:foo", "s3.acl:AuthenticatedRead")
				.applyTo(context);
		context.register(Conf.class);
		context.refresh();
		AmazonS3SinkProperties properties = context.getBean(AmazonS3SinkProperties.class);
		assertThat(properties.getAcl(), equalTo(CannedAccessControlList.AuthenticatedRead));
		context.close();
	}

	@Test
	public void aclExpressionCanBeCustomized() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

		TestPropertyValues.of("S3_ACL_EXPRESSION:headers.acl")
				.applyTo(context.getEnvironment(), TestPropertyValues.Type.SYSTEM_ENVIRONMENT);

		TestPropertyValues.of("s3.bucket:foo")
				.applyTo(context);

		context.register(Conf.class);
		context.refresh();
		AmazonS3SinkProperties properties = context.getBean(AmazonS3SinkProperties.class);
		assertThat(properties.getAclExpression().getExpressionString(), equalTo("headers.acl"));
		context.close();
	}

	@Test
	public void aclAndAclExpressionAreMutuallyExclusive() {
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		TestPropertyValues.of("s3.bucket:foo", "s3.acl:private", "s3.acl-expression:'acl'")
				.applyTo(context);
		context.register(Conf.class);
		try {
			context.refresh();
			fail("BeanCreationException expected");
		}
		catch (Exception e) {
			assertThat(e, instanceOf(BeanCreationException.class));
			assertThat(e.getCause(), instanceOf(BindException.class));
			assertThat(e.getCause().getCause(), instanceOf(BindValidationException.class));
		}
		context.close();
	}

	@EnableConfigurationProperties(AmazonS3SinkProperties.class)
	@Import(SpelExpressionConverterConfiguration.class)
	static class Conf {

	}

}
