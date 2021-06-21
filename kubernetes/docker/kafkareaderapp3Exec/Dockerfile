#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ARG splice_base_image_version
FROM splicemachine/sm_k8_base:$splice_base_image_version


ARG CDH_VERSION

# Spark dependencies
ARG SPARK_VERSION
ARG HADOOP_VERSION

ARG SPLICE_BUILD
ARG STATIC_BASE_ARTIFACT_URI
ARG SPLICEMACHINE_URI
ARG SPLICE_FOLDER_NAME
ARG SPLICE_PARCEL_NAME=${SPLICE_FOLDER_NAME}-el7.parcel


ARG SHIRO_CORE_VERSION
ARG SHIRO_WEB_VERSION
ARG SPLICE_SHIRO_VERSION


ARG SHIRO_CORE_URI=$STATIC_BASE_ARTIFACT_URI/${SHIRO_CORE_VERSION}.jar
ARG SHIRO_WEB_URI=$STATIC_BASE_ARTIFACT_URI/${SHIRO_WEB_VERSION}.jar
ARG SPLICE_SHIRO_URI=$STATIC_BASE_ARTIFACT_URI/${SPLICE_SHIRO_VERSION}.jar

ARG SPARK_NAME=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
#ARG SPARK_URI=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz


ENV SPARK_HOME=/opt/spark
ENV SPARK_LOG_DIR=/var/log/spark
ENV SPARK_PID_DIR=/var/run/spark

ENV SPLICEMACHINE_HOME=/opt/splicemachine


#RUN echo $SPARK_URI
#Install spark
COPY ./lib/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp
RUN cd /tmp && \
#    wget -q $SPARK_URI && \
 tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt --owner root --group root --no-same-owner && \
   rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
 RUN cd /opt && ln -s spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

RUN \
  mkdir -p $SPARK_LOG_DIR && \
  mkdir -p $SPARK_PID_DIR

RUN ls /opt
RUN ls /opt/spark
RUN ls $SPARK_HOME

# Copy Shiro for authentication
#RUN  cd $SPARK_HOME/jars && \
#    wget -q  $SHIRO_CORE_URI      && \
#   wget -q   $SHIRO_WEB_URI   && \
#   wget -q   $SPLICE_SHIRO_URI

COPY ./lib/shiro-core-1.2.3.jar $SPARK_HOME/jars
COPY ./lib/shiro-web-1.2.3.jar $SPARK_HOME/jars
COPY ./lib/splice-shiro-2.7.0.1915.jar $SPARK_HOME/jars

#Download Splicemachine
COPY ./lib/${SPLICE_PARCEL_NAME} /tmp
RUN  cd /tmp && \
#  wget -q   ${SPLICEMACHINE_URI}  && \
  tar xzf ${SPLICE_PARCEL_NAME} && \
  mv /tmp/${SPLICE_FOLDER_NAME} $SPLICEMACHINE_HOME && \
  cp $SPLICEMACHINE_HOME/lib/*  $SPARK_HOME/jars/  &&  \
  rm ${SPLICE_PARCEL_NAME}

ENV TINI_VERSION v0.18.0
#ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /sbin/tini
COPY ./lib/tini /sbin/tini
RUN chmod +x /sbin/tini

COPY ./jars/*.jar $SPARK_HOME/jars/
COPY ./conf/ $SPARK_HOME/conf/

COPY entrypoint.sh $SPARK_HOME/bin/
WORKDIR $SPARK_HOME/conf
ENTRYPOINT ["/opt/spark/bin/entrypoint.sh"]

# Dependencies
RUN mkdir -p /root/.m2/repository/org/slf4j/slf4j-parent/1.7.30/
COPY ./lib/slf4j-parent-1.7.30.jar /root/.m2/repository/org/slf4j/slf4j-parent/1.7.30/
COPY ./lib/slf4j-parent-1.7.30.pom /root/.m2/repository/org/slf4j/slf4j-parent/1.7.30/

RUN mkdir -p /root/.m2/repository/org/sonatype/oss/oss-parent/9/
COPY ./lib/oss-parent-9.jar /root/.m2/repository/org/sonatype/oss/oss-parent/9/
COPY ./lib/oss-parent-9.pom /root/.m2/repository/org/sonatype/oss/oss-parent/9/

RUN mkdir -p /root/.m2/repository/org/apache/commons/commons-parent/48/
COPY ./lib/commons-parent-48.jar /root/.m2/repository/org/apache/commons/commons-parent/48/
COPY ./lib/commons-parent-48.pom /root/.m2/repository/org/apache/commons/commons-parent/48/

RUN mkdir -p /root/.m2/repository/org/apache/apache/18/
COPY ./lib/apache-18.jar /root/.m2/repository/org/apache/apache/18/
COPY ./lib/apache-18.pom /root/.m2/repository/org/apache/apache/18/

RUN mkdir -p /root/.m2/repository/org/apache/apache/21/
COPY ./lib/apache-21.jar /root/.m2/repository/org/apache/apache/21/
COPY ./lib/apache-21.pom /root/.m2/repository/org/apache/apache/21/

RUN mkdir -p /root/.m2/repository/org/apache/spark/spark-parent_2.12/3.0.1/
COPY ./lib/spark-parent_2.12-3.0.1.jar /root/.m2/repository/org/apache/spark/spark-parent_2.12/3.0.1/
COPY ./lib/spark-parent_2.12-3.0.1.pom /root/.m2/repository/org/apache/spark/spark-parent_2.12/3.0.1/

RUN mkdir -p /root/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.1/
COPY ./lib/spark-sql-kafka-0-10_2.12-3.0.1.jar /root/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.1/
COPY ./lib/spark-sql-kafka-0-10_2.12-3.0.1.pom /root/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.1/

RUN mkdir -p /root/.m2/repository/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.1/
COPY ./lib/spark-token-provider-kafka-0-10_2.12-3.0.1.jar /root/.m2/repository/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.1/
COPY ./lib/spark-token-provider-kafka-0-10_2.12-3.0.1.pom /root/.m2/repository/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.1/

RUN mkdir -p /root/.m2/repository/org/apache/kafka/kafka-clients/2.4.1/
COPY ./lib/kafka-clients-2.4.1.jar /root/.m2/repository/org/apache/kafka/kafka-clients/2.4.1/
COPY ./lib/kafka-clients-2.4.1.pom /root/.m2/repository/org/apache/kafka/kafka-clients/2.4.1/

RUN mkdir -p /root/.m2/repository/org/apache/commons/commons-pool2/2.6.2/
COPY ./lib/commons-pool2-2.6.2.jar /root/.m2/repository/org/apache/commons/commons-pool2/2.6.2/
COPY ./lib/commons-pool2-2.6.2.pom /root/.m2/repository/org/apache/commons/commons-pool2/2.6.2/

RUN mkdir -p /root/.m2/repository/org/spark-project/spark/unused/1.0.0/
COPY ./lib/unused-1.0.0.jar /root/.m2/repository/org/spark-project/spark/unused/1.0.0/
COPY ./lib/unused-1.0.0.pom /root/.m2/repository/org/spark-project/spark/unused/1.0.0/

RUN mkdir -p /root/.m2/repository/com/github/luben/zstd-jni/1.4.4-3/
COPY ./lib/zstd-jni-1.4.4-3.jar /root/.m2/repository/com/github/luben/zstd-jni/1.4.4-3/
COPY ./lib/zstd-jni-1.4.4-3.pom /root/.m2/repository/com/github/luben/zstd-jni/1.4.4-3/

RUN mkdir -p /root/.m2/repository/org/lz4/lz4-java/1.7.1/
COPY ./lib/lz4-java-1.7.1.jar /root/.m2/repository/org/lz4/lz4-java/1.7.1/
COPY ./lib/lz4-java-1.7.1.pom /root/.m2/repository/org/lz4/lz4-java/1.7.1/

RUN mkdir -p /root/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.5/
COPY ./lib/snappy-java-1.1.7.5.jar /root/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.5/
COPY ./lib/snappy-java-1.1.7.5.pom /root/.m2/repository/org/xerial/snappy/snappy-java/1.1.7.5/

RUN mkdir -p /root/.m2/repository/org/slf4j/slf4j-api/1.7.30/
COPY ./lib/slf4j-api-1.7.30.jar /root/.m2/repository/org/slf4j/slf4j-api/1.7.30/
COPY ./lib/slf4j-api-1.7.30.pom /root/.m2/repository/org/slf4j/slf4j-api/1.7.30/
